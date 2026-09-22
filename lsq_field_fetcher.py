# ══════════════════════════════════════════════════════════════════════════════
# LSQ Lens  –  Streamlit app
# ══════════════════════════════════════════════════════════════════════════════
#  Install :  pip install -r requirements.txt
#  Run     :  streamlit run lsq_field_fetcher.py
#
#  How the speed-up works (Streamlit itself is single-threaded, but the slow part
#  of this job is waiting on the network, which threads handle very well):
#    1. Worker threads ONLY make HTTP calls – they never touch st.* – so there is
#       no "missing ScriptRunContext" problem.
#    2. The main Streamlit thread drives the stqdm progress bar as results arrive.
#    3. Every worker thread keeps its own requests.Session (connection re-use =
#       no TLS handshake per call).
#    4. Duplicate keys are looked up once; fallback keys (2nd / 3rd priority) are
#       only queried for rows that are still blank.
#    5. A shared sliding-window limiter (max 30 calls / 5 s) + exponential back-off keeps
#       LeadSquared happy; every retry is counted against the limit too.
# ══════════════════════════════════════════════════════════════════════════════

# ── Imports ───────────────────────────────────────────────────────────────────
import io                                           # in-memory file buffers for uploads
import json                                         # read / write the credentials JSON
import random                                       # jitter for retry back-off
import threading                                    # thread-local sessions and locks
import time                                         # sleeping and timing
from collections import deque                       # timestamps for the rate limiter
from concurrent.futures import ThreadPoolExecutor, as_completed  # parallel HTTP calls
from dataclasses import dataclass                   # tidy container for credentials

import pandas as pd                                 # tables
import requests                                     # HTTP client
import streamlit as st                              # the UI framework
from requests.adapters import HTTPAdapter           # lets us tune the connection pool
from stqdm import stqdm                             # Streamlit-native tqdm progress bar

# ── Settings you may want to tweak ────────────────────────────────────────────
DEFAULT_HOST = "api-in21.leadsquared.com"           # default LeadSquared API host (India region)
MAX_ROWS = 20_000                                   # maximum rows accepted per uploaded file
REQUEST_TIMEOUT = 15                                # seconds to wait for one API response
MAX_RETRIES = 3                                     # attempts per API call before giving up
BACKOFF_BASE = 1.5                                  # first retry waits ~1.5 s, then 3 s, 6 s …
RATE_LIMIT_CALLS = 30                               # LeadSquared allows at most 30 API calls …
RATE_LIMIT_WINDOW = 5.0                             # … in any rolling 5-second window
SEARCH_KEYS = ["Prospect ID", "Email Address", "Phone Number"]  # keys we can search leads by
AUTH_HINTS = ("invalid credential", "invalid access", "invalid secret",
              "unauthorized", "authentication")     # words in an error body that mean "bad keys"

# ── How to pick a value when a lead has several activities ────────────────────
PICK_LATEST = "Latest non-blank value"              # newest activity that has a value
PICK_EARLIEST = "Earliest non-blank value"          # oldest activity that has a value
PICK_ALL = "All distinct values (joined with ' | ')"  # every different value, in one cell
PICK_MODES = [PICK_LATEST, PICK_EARLIEST, PICK_ALL]  # order shown in the selectbox

# ── Field dictionaries  {name shown in the UI : API field name} ───────────────
# Generated from LSQ_Fields.xlsx. Add more lines here in the same "Label": "ApiName" format.
# Comments after a line are the notes from your workbook (things worth double-checking).
LEAD_FIELDS = {
    "Service Owner": "ServiceOwner",
    "Prospect ID": "ProspectID",
    "Lead Number": "ProspectAutoId",
    "Source Referrer": "SourceReferrer",
    "Last Notable Activity": "NotableEvent",
    "Last Notable Activity Date": "NotableEventdate",
    "Last Visit Date": "LastVisitDate",
    "Related Landing Page Id": "RelatedLandingPageId",
    "First Landing Page Submission Id": "FirstLandingPageSubmissionId",
    "First Landing Page Submission Date": "FirstLandingPageSubmissionDate",
    "Last Activity": "ProspectActivityName_Max",
    "Last Activity Date": "ProspectActivityDate_Max",
    "First Activity Date": "ProspectActivityDate_Min",
    "Web Referrer": "Web_Referrer",
    "Web Referrer Keyword": "Web_RefKeyword",
    "Recently Modified On": "LeadLastModifiedOn",
    "Conversion Referrer URL": "ConversionReferrerURL",
    "Source Referrer URL": "SourceReferrerURL",
    "Source IP Address": "SourceIPAddress",
    "Latitude": "Latitude",
    "Longitude": "Longitude",
    "First Name": "FirstName",
    "Last Name": "LastName",
    "Email": "EmailAddress",
    "Phone Number": "Phone",
    "Company": "Company",
    "Website": "Website",
    "Do Not Email": "DoNotEmail",
    "Do Not Track": "DoNotTrack",
    "Do Not SMS": "DoNotSMS",
    "Do Not Call": "DoNotCall",
    "Lead Source": "Source",
    "Mobile Number": "Mobile",
    "Source Campaign": "SourceCampaign",
    "Source Medium": "SourceMedium",
    "Job Title": "JobTitle",
    "Source Content": "SourceContent",
    "Notes": "Notes",
    "Lead Score": "Score",
    "Engagement Score": "EngagementScore",
    "Order Value": "Revenue",
    "Lead Stage": "ProspectStage",
    "Lead Quality": "QualityScore01",
    "Created By": "CreatedBy",
    "Created On": "CreatedOn",
    "Modified By": "ModifiedBy",
    "Modified On": "ModifiedOn",
    "Time Zone": "TimeZone",
    "TotalVisits": "TotalVisits",
    "Page Views Per Visit": "PageViewsPerVisit",
    "Average Time Per Visit": "AvgTimePerVisit",
    "City": "mx_City",
    "State": "mx_State",
    "Country": "mx_Country",
    "Zip": "mx_Zip",
    "Owner Email": "OwnerIdEmailAddress",
    "Lead Origin": "Origin",
    "Mailing Preferences": "MailingPreferences",
    "Twitter": "TwitterId",
    "Photo Url": "PhotoUrl",
    "LinkedIn": "LinkedInId",
    "Skype Name": "SkypeId",
    "Gtalk User": "GTalkId",
    "Current Opt In Status": "CurrentOptInStatus",
    "Opt In Date": "OptInDate",
    "Opt In Details": "OptInDetails",
    "Last Opt In Email Sent Date": "LastOptInEmailSentDate",
    "Lead Age": "LeadAge",
    "Course": "mx_Course",
    "Payment Type": "mx_Payment_Type",
    "Profession": "mx_Profession",
    "Attended": "mx_Attended",
    "Gender": "mx_Gender",
    "WhatsApp Number": "mx_WhatsApp_Number",
    "Inbound Calls": "mx_Inbound_Calls",
    "Outbound Calls": "mx_Outbound_Calls",
    "RNR": "mx_RNR",
    "Follow Up Date": "mx_Follow_Up_Date",
    "Last Call Notes": "mx_Last_Call_Notes",
    "Part Payment Amount": "mx_Part_Payment_Amount",
    "Workshop Batch Name": "mx_Workshop_Batch_Name",
    "Live or Automated": "mx_Live_or_Automated",
    "Price Point": "mx_Price_Point",
    "Day": "mx_Day",
    "Course Details": "mx_Course_Details",
    "Product Details": "mx_Product_Details",
    "Order Status": "mx_Order_Status",
    "woocomerce orderid": "mx_woocomerce_orderid",
    "woocommerce amount": "mx_woocommerce_amount",
    "BDA Payment Platform Name": "mx_BDA_Payment_Platform_Name",
    "Non razorpay mastery payment": "mx_Non_razorpay_mastery_payment",
    "Mastery amount": "mx_Mastery_amount",
    "Token amount": "mx_Token_amount",
    "Token_transId": "mx_Token_transId",
    "Mastery_transId": "mx_Mastery_transId",
    "old_Token_transId": "mx_old_Token_transId",
    "old_Mastery_transId": "mx_old_Mastery_transId",
    "Payment Status": "mx_Payment_Status",
    "Payment status workshop": "mx_Payment_status_workshop",
    "Payment status mastery": "mx_Payment_status_mastery",
    "Abandoned Cart": "mx_Abandened_Cart",  # API name has a typo (Abandened) - use it exactly as listed.
    "token success signal": "mx_token_success_signal",
    "mastery success signal": "mx_mastery_success_signal",
    "Closed Date": "mx_Closed_Date",
    "Mastery Amount Counter": "mx_Mastery_Amount_Counter",
    "Occupation": "mx_Occupation",
    "Follow Up Status": "mx_Follow_Up_Status",
    "Follow Up Counter": "mx_Follow_Up_Counter",
    "Workshop Date and Time": "mx_Workshop_Date_and_Time",
    "Count Inbound": "mx_Count_Inbound",
    "Count Outbound": "mx_Count_Outbound",
    "mail to be sent": "mx_mail_to_be_sent",
    "Lead Assigned Date Time": "mx_Lead_Assigned_Date_Time",
    "time in session": "mx_time_in_session",
    "Previous Owner": "mx_Previous_Owner",
    "Last Call Date": "mx_Last_Call_Date",
    "Preapproved from FlexMoney": "mx_Preapproved_from_FlexMoney",
    "Collection Batch Name": "mx_Collection_Batch_Name",
    "Session Schedule": "mx_Session_Schedule",
    "Is Portal User": "mx_Portal_IsPortalUser",
    "Application Form Filled": "mx_Application_Form_Filled",
    "Age": "mx_Age",
    "Background_Struggles_Willingness for LWB": "mx_Background_Struggles_Willingness_for_LWB",
    "Upsell Form Filled": "mx_Upsell_Form_Filled",
    "Current Workshop Batch Name": "mx_Current_Workshop_Batch_Name",
    "Monthly Salary": "mx_Monthly_Salary",
    "Hold Credit Card": "mx_Hold_Credit_Card",
    "amount fetched": "mx_amount_fetched",
    "Age Achieved Financial independence": "mx_Age_Achieved_Financial_independence",
    "Monthly income from stock market": "mx_Monthly_income_from_stock_market",
    "Partial Preapproved from FlexMoney": "mx_Partial_Preapproved_from_FlexMoney",
    "Refunded": "mx_Refunded",
    "LCP Batch Name": "mx_LCP_Batch_Name",
    "LCP Batch LMS Link": "mx_LCP_Batch_LMS_Link",
    "kundli sent on": "mx_kundli_sent_on",
    "Highest Education": "mx_Highest_Education",
    "Education or Work Institute": "mx_Education_or_Work_Institute",
    "Education or Work Industry": "mx_Education_or_Work_Industry",
    "Flex Subscription Status": "mx_Flex_Subscription_Status",
    "Flex Bounced": "mx_Flex_Bounced",
    "Telegram Link QnA": "mx_Telegram_Link_QnA",
    "Telegram Link Session": "mx_Telegram_Link_Session",
    "Broadcast Email": "mx_Broadcast_Email",
    "Country Code": "mx_Country_Code",
    "place of birth": "mx_place_of_birth",
    "Your Age": "mx_Your_Age",
    "missed to attend on": "mx_missed_to_attend_on",
    "brdcst payment amount": "mx_brdcst_payment_amount",
    "brdcst payment link": "mx_brdcst_payment_link",
    "brdcst pitch fee": "mx_brdcst_pitch_fee",
    "brdcst nxt emi amount": "mx_brdcst_nxt_emi_amount",
    "brdcst nxt emi date": "mx_brdcst_nxt_emi_date",
    "brdcst paid group link": "mx_brdcst_paid_group_link",
    "brdcst workshop whatsapp link": "mx_brdcst_workshop_whatsapp_link",
    "Kundli Link": "mx_Kundli_Link",
    "Sat Link LCP": "mx_Sat_Link_LCP",
    "Sun Link LCP": "mx_Sun_Link_LCP",
    "Workshop Amount": "mx_Workshop_Amount",
    "PaymentSlug": "mx_PaymentSlug",
    "What industry are you in": "mx_What_industry_are_you_in",
    "current job title or role in your company": "mx_current_job_title_or_role_in_your_company",
    "current Salary per annum": "mx_current_Salary_per_annum",
    "Type of Query": "mx_Type_of_Query",
    "salesform_Intro to attend by customer": "mx_salesform_Intro_to_attend_by_customer",
    "salesform_ bda name": "mx_salesform__bda_name",
    "salesform_Provide LMS Course Access": "mx_salesform_Provide_LMS_Course_Access",
    "salesform_sold course name": "mx_salesform_sold_course_name",
    "salesform_Remark": "mx_salesform_Remark",
    "salesform_Closing Amount": "mx_salesform_Closing_Amount",
    "salesform_REST COLLECTION": "mx_salesform_REST_COLLECTION",
    "Date of Birth": "mx_Date_of_Birth",
    "Field of Study": "mx_Field_of_Study",
    "workshop date passed": "mx_worshop_date_passed",  # API name has a typo (worshop) - use it exactly as listed.
    "Inbound Intent": "mx_Inbound_Intent",
    "OTO_NonOTO": "mx_OTO_NonOTO",
    "Funnel Name": "mx_Funnel_Name",
    "FINANCE TYPE": "mx_FINANCE_TYPE",
    "Finance Amount": "mx_Finance_Amount",
    "BDM NAME": "mx_BDM_NAME",
    "Sale Amount": "mx_Sale_Amount",
    "PRODUCT SOLD": "mx_PRODUCT_SOLD",
    "Trigger Automation": "mx_Trigger_Automation",
    "zoomads": "mx_zoomads",
    "brdcst calendar link": "mx_brdcst_calendar_link",
    "brdcst L1 certificate link": "mx_brdcst_L1_certificate_link",
    "brdcst L1 notes link": "mx_brdcst_L1_notes_link",
    "Current Owner": "mx_Current_Owner",
    "salesform collection of": "mx_salesform_collection_of",
    "sales_emp_id": "mx_sales_emp_id",
    "Campaign ID": "mx_Campaign_ID",
    "Type of Lead": "mx_Type_of_Lead",
    "Zoom Link": "mx_Zoom_Link",
    "Webinar ID": "mx_Webinar_ID",
    "Payment Failed Lead": "mx_Payment_Failed_Lead",
    "lwb lms link": "mx_lwb_lms_link",
    "LWB Cohort type": "mx_LWB_Cohort_type",
    "Workshop Registration Date": "mx_Workshop_Registration_Date",
    "LWB_GWM access level": "mx_LWB_GWM_access_level",
    "Lead Closed by": "mx_Lead_Closed_by",
    "Lead Rating": "mx_Lead_Rating",
    "Pitched payment amount": "mx_pitched_payment_amount",
    "Form_Name": "mx_Form_Name",
    "Objection By Customer": "mx_Objection_By_Customer",
    "Urgent Call Requests": "mx_Urgent_Call_Requests",
    "Intro Date Passed": "mx_Intro_Date_Passed",
    "Lead To be transferred": "mx_Lead_To_be_transferred",
    "Intro Date": "mx_Intro_Date",
    "Zoom ID": "mx_Zoom_ID",
    "L1 Credential ID": "mx_L1_Credential_ID",
    "Workshop Date": "mx_Workshop_Date",
    "Alternate Email": "mx_Alternate_Email",
    "Alternate Phone Number": "mx_Alternate_Phone_Number",
    "occasion": "mx_occasion",
    "Experience": "mx_Experience",
    "Reason for joining workshop": "mx_Reason_for_joining_workshop",
    "Instacred Eligibility URL": "mx_Instacred_Eligibility_URL",
    "Instacred Financing Options": "mx_Instacred_Financing_Options",
    "Is Financing Available": "mx_Is_Financing_Available",
    "Instacred Limit Amount": "mx_Instacred_Limit_Amount",
    "Lead stage changed on": "mx_Lead_stage_changed_on",
    "Name as per PAN": "mx_Name_as_per_PAN",
    "AI_Reengagement_Status": "mx_AI_Reengagement_Status",
    "AI_Bucket": "mx_AI_Bucket",
    "Exotic": "mx_Exotic",
    "Onboarding Zoom Link": "mx_Onboarding_Zoom_Link",
    "Intro collection": "mx_Intro_collection",
    "Onboarding Rec Link": "mx_Onboarding_Rec_Link",
    "Preferred Batch L2": "mx_Preferred_Batch_L2",
    "Bulls Coup Code": "mx_Bulls_Coup_Code",
    "Divinelead": "mx_Divinelead",
    "Time in Session of Day 2": "mx_Time_in_Session_of_Day_2",
    "InstaCred Reference ID": "mx_InstaCred_Reference_ID",
    "Current Owner EmailAddress": "mx_Current_Owner_EmailAddress",
    "Intro Time in session": "mx_Intro_Time_in_session",
    "Position": "mx_Position",
    "To Distribute": "mx_To_Distribute",
    "Age Bucket": "mx_Age_Bucket",
    "zoom ads": "mx_zoom_ads",
    "Pre Owner email": "mx_Pre_Owner_email",
    "To Be Shared": "mx_To_Be_Shared",
    "Time": "mx_Time",
    "lead under AVP": "mx_lead_under_AVP",
    "Query": "mx_Query",
    "Session Engagement": "mx_Session_Engagement",
    "Agent ID": "mx_Agent_ID",
    "Hooman Campaign ID": "mx_Hooman_Campaign_ID",
    "Expected Arpu": "mx_Expected_Arpu",
    "Parent or Guardian Phone": "mx_Parent_or_Guardian_Phone",
    "DVL Quantity": "mx_DVL_Quantity",
    "DVL Product": "mx_DVL_Product",
}

ACTIVITY_FIELDS = {
    "Prospect Id": "ProspectID",
    "Email Address": "EmailAddress",
    "Phone Number": "Phone",
    "Notes": "Notes",
    "Collection Batch Name": "mx_Collection_Batch_Name",
    "Previous Owner": "mx_Previous_Owner",
    "time in session": "mx_time_in_session",
    "Intro Time in Session": "mx_Intro_Time_in_session",
    "Lead Stage": "ProspectStage",
    "Pre-approved from flex money": "mx_Preapproved_from_FlexMoney",
    "funnel name": "mx_Funnel_Name",
    "workshop date and time": "mx_Workshop_Date_and_Time",
    "Workshop Batch Name": "mx_Workshop_Batch_Name",
    "owner email": "OwnerIdEmailAddress",
    "Lead Number": "ProspectAutoId",
    "Email": "EmailAddress",
    "Workshop batch Name": "mx_Workshop_Batch_Name",
    "Payment Status": "mx_Payment_Status",
    "Owner (User ID)": "OwnerId",  # Owner GUID; not listed under any Lead display name
    "Owner (User Name)": "OwnerIdName",  # Same field the Lead sheet calls Owner
    "Owner (User Email)": "OwnerIdEmailAddress",  # Same field the Lead sheet calls Owner Email
    "attended status": "mx_Attended",  # Possibly the Attended field - please confirm
    "cust email": "EmailAddress",  # Customer's email
    "cust phone": "Phone",  # Customer's phone
    "prev owner email": "mx_Pre_Owner_email",  # Looks like the previous-owner email field
    "BDM": "mx_BDM_NAME",  # BDM name field
    "Lead Created On": "CreatedOn",  # Lead creation timestamp
    "Lead Owner Email": "OwnerIdEmailAddress",  # Owner email under a different label
}


# ══════════════════════════════════════════════════════════════════════════════
#  1. Credentials
# ══════════════════════════════════════════════════════════════════════════════
@dataclass                                          # auto-generates __init__ for us
class Credentials:
    access_key: str                                 # LeadSquared access key
    secret_key: str                                 # LeadSquared secret key
    host: str = DEFAULT_HOST                        # API host, defaults to the India region


def normalise_host(host: str) -> str:
    """Strip 'https://' and trailing slashes so users can paste a full URL."""
    host = (host or "").strip()                     # remove surrounding whitespace
    for prefix in ("https://", "http://"):          # remove either scheme prefix
        if host.lower().startswith(prefix):         # check the prefix case-insensitively
            host = host[len(prefix):]               # cut the prefix off
    return host.strip("/") or DEFAULT_HOST          # fall back to the default if nothing is left


def parse_credentials(raw: bytes) -> Credentials:
    """Turn the uploaded JSON bytes into a Credentials object (raises ValueError if invalid)."""
    try:
        data = json.loads(raw.decode("utf-8-sig"))  # utf-8-sig also copes with a BOM
    except (ValueError, UnicodeDecodeError) as exc:  # not valid JSON at all
        raise ValueError(f"Credentials file is not valid JSON ({exc}).") from exc
    if not isinstance(data, dict):                  # JSON must be an object {...}
        raise ValueError("Credentials JSON must be an object with ACCESS_KEY and SECRET_KEY.")
    upper = {str(k).upper(): v for k, v in data.items()}  # make key names case-insensitive
    access, secret = upper.get("ACCESS_KEY"), upper.get("SECRET_KEY")  # the two required values
    if not access or not secret:                    # both must be present and non-empty
        raise ValueError("Credentials JSON must contain ACCESS_KEY and SECRET_KEY.")
    return Credentials(str(access).strip(), str(secret).strip(),
                       normalise_host(upper.get("HOST") or DEFAULT_HOST))  # HOST is optional


# ══════════════════════════════════════════════════════════════════════════════
#  2. Small helpers
# ══════════════════════════════════════════════════════════════════════════════
def is_blank(value) -> bool:
    """True for None, NaN, empty strings and whitespace-only strings."""
    if value is None:                               # a missing value
        return True
    if isinstance(value, float) and value != value:  # NaN is the only float not equal to itself
        return True
    return str(value).strip() == ""                 # empty or spaces only


def clean(value) -> str:
    """Convert any API value to a tidy string ('' when blank)."""
    return "" if is_blank(value) else str(value).strip()


def normalise_key(key_type: str, value) -> str:
    """Prepare a raw cell for searching (strip spaces, remove Excel's trailing '.0')."""
    text = clean(value)                             # '' when blank
    if text.endswith(".0") and text[:-2].replace("+", "").isdigit():  # e.g. 919819182249.0
        text = text[:-2]                            # drop the '.0'
    if key_type == "Phone Number":                  # phone numbers: remove spaces inside the number
        text = text.replace(" ", "")
    return text


def flatten_lead(lead: dict) -> dict:
    """Return a lower-cased {field: value} dict from either LeadSquared response layout."""
    flat = {str(k).lower(): v for k, v in lead.items() if k != "LeadPropertyList"}  # flat layout
    for prop in lead.get("LeadPropertyList") or []:  # "Attribute/Value" list layout
        if isinstance(prop, dict) and prop.get("Attribute"):  # only well-formed entries
            flat[str(prop["Attribute"]).lower()] = prop.get("Value")  # add to the flat dict
    return flat


def flatten_activity(activity: dict) -> dict:
    """Flatten one activity record into a lower-cased {field: value} dict."""
    flat = {}                                       # result being built
    for key, value in activity.items():             # top-level scalar fields (CreatedOn, Notes …)
        if not isinstance(value, (list, dict)):     # skip nested structures here
            flat[str(key).lower()] = value          # store with a lower-cased key
    for list_key in ("Data", "ActivityFields", "Fields", "CustomFields"):  # known field lists
        for item in activity.get(list_key) or []:   # each item is a small dict
            if not isinstance(item, dict):          # ignore anything unexpected
                continue
            name = (item.get("Key") or item.get("SchemaName")
                    or item.get("Attribute") or item.get("Name"))  # whichever name is used
            if name:                                # only store items that have a name
                flat[str(name).lower()] = item.get("Value")  # keep the value
    return flat


def extract_activities(payload) -> list:
    """Accept the different shapes the Activity API can return and give back a list of flat dicts."""
    if isinstance(payload, list):                   # already a list of activities
        items = payload
    elif isinstance(payload, dict):                 # wrapped in an object
        items = next((payload[k] for k in ("ProspectActivities", "Activities", "List", "Data")
                      if isinstance(payload.get(k), list)), [])  # first list-valued wrapper key
    else:                                           # None / string / anything else
        items = []
    return [flatten_activity(a) for a in items if isinstance(a, dict)]  # flatten each activity


def pick_activity_values(activities: list, specs: list, pick_mode: str) -> dict:
    """From several activities, choose the value(s) for each requested field."""
    ordered = sorted(activities, key=lambda a: str(a.get("createdon") or a.get("activitydatetime") or ""),
                     reverse=True)                  # newest first (ISO timestamps sort as text)
    if pick_mode == PICK_EARLIEST:                  # earliest wanted -> flip the order
        ordered.reverse()
    result = {}                                     # {display label: value}
    for label, api_name in specs:                   # every requested field
        found = []                                  # non-blank values in priority order
        for act in ordered:                         # walk through the activities
            value = clean(act.get(api_name.lower(), act.get(label.lower())))  # API name, else label
            if value:                               # keep only non-blank values
                found.append(value)
        if pick_mode == PICK_ALL:                   # join every distinct value
            result[label] = " | ".join(dict.fromkeys(found))  # dict.fromkeys de-duplicates in order
        else:                                       # latest / earliest -> first hit
            result[label] = found[0] if found else ""
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  3. LeadSquared API client (thread-safe)
# ══════════════════════════════════════════════════════════════════════════════
class ApiError(Exception):
    """A lookup failed after retries (timeouts, 5xx, bad JSON). Only that one row is affected."""


class AuthError(Exception):
    """Credentials were rejected – there is no point continuing the run."""


class RateLimiter:
    """Sliding-window limiter: never more than `max_calls` calls in any `window` seconds (all threads)."""

    def __init__(self, max_calls: int, window: float):
        self.max_calls = max(1, int(max_calls))     # calls allowed per window
        self.window = float(window)                 # window length in seconds
        self.lock = threading.Lock()                # one thread at a time may claim a slot
        self.stamps = deque()                       # start times of the most recent calls

    def wait(self) -> None:
        with self.lock:                             # holding the lock makes other threads queue up
            while True:                             # loop until a slot is free
                now = time.monotonic()              # current clock
                while self.stamps and now - self.stamps[0] >= self.window:  # forget calls older than the window
                    self.stamps.popleft()
                if len(self.stamps) < self.max_calls:  # room left in the window?
                    self.stamps.append(now)         # claim the slot and go
                    return
                time.sleep(self.window - (now - self.stamps[0]) + 0.01)  # sleep until the oldest call expires


_thread_local = threading.local()                   # storage that is separate for each thread


def get_session() -> requests.Session:
    """One requests.Session per worker thread, so its connection is re-used (keep-alive)."""
    if not hasattr(_thread_local, "session"):       # first call in this thread
        session = requests.Session()                # create the session
        session.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=2))  # small pool
        _thread_local.session = session             # remember it for next time
    return _thread_local.session                    # hand it back


class LsqClient:
    """Wraps the LeadSquared endpoints used by the app."""

    def __init__(self, creds: Credentials, max_calls: int = RATE_LIMIT_CALLS):
        self.creds = creds                          # keys + host
        self.limiter = RateLimiter(max_calls, RATE_LIMIT_WINDOW)  # shared by every thread
        self._lock = threading.Lock()               # protects the counters below
        self.calls = 0                              # total HTTP calls made
        self.retries = 0                            # how many of them were retries

    def _bump(self, attr: str) -> None:
        with self._lock:                            # counters are shared, so lock while updating
            setattr(self, attr, getattr(self, attr) + 1)

    def _back_off(self, attempt: int) -> None:
        if attempt < MAX_RETRIES - 1:               # no point sleeping after the last attempt
            self._bump("retries")                   # record the retry
            time.sleep(BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 0.5))  # exponential + jitter

    def _request(self, method: str, path: str, params=None, body=None):
        """Call the API with retries. Returns parsed JSON, or None for a per-row client error."""
        url = f"https://{self.creds.host}{path}"    # full endpoint URL
        query = {"accessKey": self.creds.access_key, "secretKey": self.creds.secret_key}  # auth
        query.update(params or {})                  # add endpoint-specific parameters
        session = get_session()                     # this thread's session
        last_error = "unknown error"                # remembered for the final error message
        for attempt in range(MAX_RETRIES):          # try a few times
            self.limiter.wait()                     # respect the global request rate
            self._bump("calls")                     # count the call
            try:
                if method == "GET":                 # GET endpoints (lead lookups)
                    resp = session.get(url, params=query, timeout=REQUEST_TIMEOUT)
                else:                               # POST endpoints (activities)
                    resp = session.post(url, params=query, json=body or {}, timeout=REQUEST_TIMEOUT)
            except (requests.Timeout, requests.ConnectionError) as exc:  # network trouble
                last_error = type(exc).__name__     # e.g. "ReadTimeout"
                self._back_off(attempt)             # wait, then loop to retry
                continue
            text = resp.text[:300].lower()          # start of the body, for error sniffing
            if resp.status_code in (401, 403) or (resp.status_code >= 400 and any(h in text for h in AUTH_HINTS)):
                raise AuthError(f"LeadSquared rejected the credentials (HTTP {resp.status_code}).")
            if resp.status_code == 429 or resp.status_code >= 500:  # rate-limited or server error
                last_error = f"HTTP {resp.status_code}"  # remember why
                self._back_off(attempt)             # wait, then retry
                continue
            if resp.status_code >= 400:             # other client errors: bad input for this row
                return None                         # treat as "not found"
            try:
                return resp.json()                  # success – parsed body
            except ValueError as exc:               # body was not JSON
                raise ApiError("Response was not valid JSON.") from exc
        raise ApiError(f"Gave up after {MAX_RETRIES} attempts ({last_error}).")

    def lookup_lead(self, key_type: str, value: str):
        """Fetch one lead (flattened, lower-cased keys) by Prospect ID / email / phone, or None."""
        if key_type == "Email Address":             # endpoint + parameter names from your script
            data = self._request("GET", "/v2/LeadManagement.svc/Leads.GetbyEmailAddress",
                                 {"emailAddress": value})
        elif key_type == "Phone Number":
            data = self._request("GET", "/v2/LeadManagement.svc/RetrieveLeadByPhoneNumber",
                                 {"phone": value})
        else:                                       # Prospect ID
            data = self._request("GET", "/v2/LeadManagement.svc/Leads.GetById", {"id": value})
        if isinstance(data, list):                  # these endpoints return a list of matches
            data = data[0] if data else None        # use the first match
        if not isinstance(data, dict) or not data:  # nothing usable came back
            return None
        return flatten_lead(data)                   # normalise the layout

    def get_activities(self, lead_id: str):
        """Fetch every activity for a lead, as a list of flat dicts."""
        data = self._request("POST", "/v2/ProspectActivity.svc/Retrieve", {"leadId": lead_id}, {})
        return extract_activities(data)             # list of flat dicts (may be empty)

def fetch_values(client, key_type, value, specs, mode, pick_mode, keep_raw=False):
    """Look up ONE key and return ({label: value} or None if not found, raw response or None).

    specs = [(display label, API field name), …]   mode = "Lead" or "Activity"
    """
    if mode == "Lead":                              # ── Lead fields ──
        lead = client.lookup_lead(key_type, value)  # one API call
        if lead is None:                            # no such lead
            return None, None
        if key_type == "Phone Number" and any(api.lower() not in lead for _, api in specs):
            lead_id = clean(lead.get("prospectid"))  # phone lookup may return a slim record,
            full = client.lookup_lead("Prospect ID", lead_id) if lead_id else None  # so fetch full lead
            lead = full or lead                     # use the full record when we got it
        values = {label: clean(lead.get(api.lower())) for label, api in specs}  # requested fields
        return values, (lead if keep_raw else None)  # only keep the big raw dict when asked

    if key_type == "Prospect ID":                   # ── Activity fields ──  activities need a lead id
        lead_id = value                             # already have it
    else:                                           # otherwise resolve the id from email / phone
        lead = client.lookup_lead(key_type, value)  # one extra API call
        lead_id = clean(lead.get("prospectid")) if lead else ""
    if not lead_id:                                 # could not identify the lead
        return None, None
    activities = client.get_activities(lead_id)  # activities for that lead
    if not activities:                              # lead has no (matching) activities
        return None, None
    values = pick_activity_values(activities, specs, pick_mode)  # choose values per field
    for label, api in specs:                        # ProspectID is known even if not in activity data
        if api.lower() == "prospectid" and not values.get(label):
            values[label] = lead_id
    return values, (activities if keep_raw else None)


# ══════════════════════════════════════════════════════════════════════════════
#  4. Batch engine (threads do the HTTP, the main thread draws the progress bar)
# ══════════════════════════════════════════════════════════════════════════════
def _worker(client, key_type, value, specs, mode, pick_mode):
    """Runs inside a worker thread. Must NOT call any st.* function."""
    try:
        found, _ = fetch_values(client, key_type, value, specs, mode, pick_mode)
        return value, "ok", found                   # success (found may be None = not found)
    except AuthError as exc:                        # bad credentials -> abort the whole run
        return value, "auth", str(exc)
    except Exception as exc:                        # anything else only affects this key
        return value, "error", str(exc)


def run_batch(df, plan, specs, mode, pick_mode, client, workers, progress=stqdm):
    """Fetch the requested fields for every row of df.

    plan = [(key type, column name), …] in priority order (1st, 2nd, 3rd …).
    Returns (values per label, matched-by per row, stats dict).
    """
    total = len(df)                                 # number of rows
    labels = [label for label, _ in specs]          # output column names
    values = {label: [""] * total for label in labels}  # one list per output column
    matched = [[] for _ in range(total)]            # which key(s) supplied data for each row
    cache = {}                                      # (key type, value) -> {label: value} or None
    failed, errors = 0, []                          # failed lookups + a few sample messages
    started = time.time()                           # for the elapsed-time metric

    for key_type, column in plan:                   # one "wave" per search key, in priority order
        keys = [normalise_key(key_type, v) for v in df[column].tolist()]  # cleaned key per row
        pending = [i for i in range(total)          # rows that still need something…
                   if keys[i] and any(is_blank(values[l][i]) for l in labels)]  # …and have a key here
        todo = sorted({keys[i] for i in pending if (key_type, keys[i]) not in cache})  # unique, new keys
        if todo:                                    # skip the network entirely if nothing to look up
            with ThreadPoolExecutor(max_workers=workers) as pool:  # the thread pool
                futures = [pool.submit(_worker, client, key_type, k, specs, mode, pick_mode)
                           for k in todo]           # queue one task per unique key
                for future in progress(as_completed(futures), total=len(futures),
                                       desc=f"Looking up by {key_type}"):  # stqdm bar, updated as tasks finish
                    key, status, payload = future.result()  # (key, "ok"/"error"/"auth", data)
                    if status == "auth":            # credentials rejected
                        for f in futures:           # cancel everything not yet started
                            f.cancel()
                        raise AuthError(payload)    # surface to the UI
                    if status == "error":           # one lookup failed after retries
                        failed += 1                 # count it
                        if len(errors) < 5:         # keep a few examples for the user
                            errors.append(f"{key_type} {key}: {payload}")
                        cache[(key_type, key)] = None  # treat as not found
                    else:
                        cache[(key_type, key)] = payload  # dict of values, or None if not found
        for i in pending:                           # copy cached results into the row lists
            found = cache.get((key_type, keys[i]))  # result for this row's key
            if not found:                           # not found / failed -> try the next key later
                continue
            used = False                            # did this key add anything for this row?
            for label in labels:                    # every requested field
                if is_blank(values[label][i]) and not is_blank(found.get(label)):  # fill blanks only
                    values[label][i] = found[label]
                    used = True
            if used:                                # remember which key helped
                matched[i].append(key_type)

    stats = {                                       # numbers shown in the summary
        "rows": total,                              # rows processed
        "rows_with_data": sum(1 for i in range(total) if any(values[l][i] for l in labels)),
        "api_calls": client.calls,                  # HTTP calls actually made
        "retries": client.retries,                  # how many were retries
        "failed": failed,                           # lookups that failed after retries
        "errors": errors,                           # sample error messages
        "seconds": round(time.time() - started, 1),  # elapsed time
    }
    return values, matched, stats


def build_output(df, plan, specs, values, matched, include_all):
    """Assemble the final DataFrame that the user downloads."""
    if include_all:                                 # keep every uploaded column
        out = df.copy()
    else:                                           # keep only the search-key column(s)
        out = df[list(dict.fromkeys(col for _, col in plan))].copy()  # unique, in order
    for label, _ in specs:                          # add one column per requested field
        name = label
        while name in out.columns:                  # avoid clashing with an uploaded column
            name += " (LSQ)"
        out[name] = values[label]                   # '' where nothing was found
    if len(plan) > 1:                               # with fallback keys, show which key worked
        out["Matched By"] = [" + ".join(m) for m in matched]
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  5. Streamlit UI helpers
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="Reading file…")        # re-uploading the same file is instant
def load_table(name: str, data: bytes) -> pd.DataFrame:
    """Read a CSV / Excel upload with every column as text (keeps leading zeros and long IDs)."""
    buffer = io.BytesIO(data)                       # wrap the bytes so pandas can read them
    if name.lower().endswith(".csv"):               # CSV
        df = pd.read_csv(buffer, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    else:                                           # Excel
        df = pd.read_excel(buffer, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip() for c in df.columns]  # tidy the headers
    return df


def guess_column(columns, key_type: str) -> int:
    """Pre-select the column that most likely holds the given key type."""
    hints = {"Prospect ID": ("prospect",), "Email Address": ("email", "mail"),
             "Phone Number": ("phone", "mobile", "contact")}[key_type]  # words to look for
    for hint in hints:                              # try each hint in order
        for i, col in enumerate(columns):           # against each column name
            if hint in col.lower().replace("_", " "):
                return i                            # first match wins
    return 0                                        # nothing matched -> first column


def render_credentials_builder() -> None:
    """Panel for creating + downloading the credentials JSON."""
    st.subheader("Create a credentials file")
    st.caption("Fill in the details and download the JSON. Upload it later from the sidebar of the "
               "'Fetch fields' tab. Nothing is stored by this app.")
    access = st.text_input("ACCESS_KEY", type="password", key="new_access")  # hidden while typing
    secret = st.text_input("SECRET_KEY", type="password", key="new_secret")  # hidden while typing
    host = st.text_input("HOST", value=DEFAULT_HOST, key="new_host")  # placeholder / default host
    payload = json.dumps({"ACCESS_KEY": access.strip(), "SECRET_KEY": secret.strip(),
                          "HOST": normalise_host(host)}, indent=2)  # the file's content
    st.download_button("⬇️ Download credentials.json", data=payload, file_name="credentials.json",
                       mime="application/json", disabled=not (access.strip() and secret.strip()))
    st.warning("The downloaded file contains your keys in plain text – keep it private and "
               "never commit it to Git.")


def sticky_multiselect(label, options, key, default=None, **kwargs):
    """st.multiselect that remembers its picks even while hidden (Streamlit forgets hidden widgets)."""
    saved = st.session_state.get(f"_saved_{key}", default or [])  # last picks, else the default
    saved = [x for x in saved if x in options]      # drop anything that is no longer an option
    chosen = st.multiselect(label, options, default=saved, key=key, **kwargs)  # the real widget
    st.session_state[f"_saved_{key}"] = chosen      # remember for when the widget is shown again
    return chosen


def choose_fields(mode: str, field_map: dict) -> list:
    """Multiselect for the fields to fetch. Returns [(display label, API field name)]."""
    chosen = sticky_multiselect(f"{mode} fields to fetch (type to search)", list(field_map.keys()),
                            key=f"selected_{mode}", placeholder="Choose one or more fields…")  # separate state per mode
    if chosen:                                      # show the API name behind each label
        st.caption("  ·  ".join(f"{label} → `{field_map[label]}`" for label in chosen))
    else:
        st.info("Select at least one field to fetch.")  # gentle nudge
    return [(label, field_map[label]) for label in chosen]  # (display label, API name) pairs


def choose_key_plan(columns) -> list:
    """Multiselect for the search key(s) + a column picker per key. Returns [(key type, column)]."""
    keys = st.multiselect("Search key(s) – pick any combination of Prospect ID, Email Address and "
                          "Phone Number", SEARCH_KEYS, default=[SEARCH_KEYS[0]], key="search_keys",
                          help="Pick one key for a normal search. Pick several to use the later ones "
                               "(in the order you pick them) as fallbacks for rows still blank after "
                               "the earlier ones.")
    if not keys:                                    # nothing chosen -> nothing to search by
        st.info("Select at least one search key.")
        return []
    if len(keys) > 1:                               # make the priority order visible
        st.caption("Priority:  " + "  →  ".join(f"{i}. {k}" for i, k in enumerate(keys, start=1)))
    plan = []                                       # [(key type, file column)]
    for key_type in keys:                           # one column mapping per chosen key (a key maps to ONE column)
        column = st.selectbox(f"File column holding the {key_type}", columns,
                              index=guess_column(columns, key_type), key=f"col_{key_type}")
        plan.append((key_type, column))
    return plan


def render_result() -> None:
    """Show the stored result (kept in session_state so it survives the download-button rerun)."""
    result = st.session_state.get("result")         # nothing yet?
    if not result:
        return
    st.divider()
    st.subheader("Results")
    if result.get("single"):                        # single-lead view: field / value table
        st.dataframe(result["df"].T.rename(columns={0: "Value"}), use_container_width=True)
        if result.get("raw") is not None:           # optional raw response for debugging
            with st.expander("Raw API response"):
                st.json(result["raw"])
    else:                                           # batch view: summary metrics + preview
        s = result["stats"]                         # numbers collected by run_batch
        c1, c2, c3, c4, c5 = st.columns(5)          # one metric per column
        c1.metric("Rows", f"{s['rows']:,}")
        c2.metric("Rows with data", f"{s['rows_with_data']:,}")
        c3.metric("API calls", f"{s['api_calls']:,}")
        c4.metric("Failed lookups", f"{s['failed']:,}")
        c5.metric("Time (s)", s["seconds"])
        if s["failed"]:                             # explain failures so they are not silent
            st.warning("Some lookups failed after retries (timeouts / rate limits) and were left blank. "
                       "Lower the requests-per-second slider and run again.\n\n" + "\n".join(f"- {e}" for e in s["errors"]))
        st.dataframe(result["df"].head(200), use_container_width=True)  # first 200 rows only
    st.download_button("⬇️ Download CSV", data=result["csv"], file_name=result["file_name"],
                       mime="text/csv")             # utf-8-sig so Excel opens it correctly


# ══════════════════════════════════════════════════════════════════════════════
#  6. Main "Fetch fields" panel
# ══════════════════════════════════════════════════════════════════════════════
def render_fetch_panel(creds, workers, max_calls, show_raw) -> None:
    """Everything on the 'Fetch fields' tab."""
    use_activity = st.checkbox("Fetch **Activity** fields  (leave unticked for **Lead** fields)", key="use_activity")
    mode = "Activity" if use_activity else "Lead"   # which mode we are in
    field_map = ACTIVITY_FIELDS if use_activity else LEAD_FIELDS  # matching dictionary

    pick_mode = PICK_LATEST                          # default (unused in Lead mode)
    if use_activity:                                # extra option only Activity mode needs
        pick_mode = st.selectbox("If a lead has several matching activities, use the",
                                 PICK_MODES)         # full-width, so it lines up with the widgets around it

    specs = choose_fields(mode, field_map)          # [(label, api name), …]
    st.divider()

    source = st.radio("Search", ["Single lead", "Upload a file (CSV / Excel)"], horizontal=True)

    if source == "Single lead":                     # ── one lead ──
        key_type = st.selectbox("Search key", SEARCH_KEYS, key="single_key")
        value = st.text_input(f"{key_type}", key="single_value")  # the thing to search for
        if st.button("🔎 Fetch", type="primary", disabled=not (creds and specs and value.strip())):
            client = LsqClient(creds, max_calls)      # fresh client for this call
            search = normalise_key(key_type, value)  # tidy the input
            try:
                with st.spinner("Contacting LeadSquared…"):
                    found, raw = fetch_values(client, key_type, search, specs, mode,
                                              pick_mode, keep_raw=show_raw)
            except AuthError as exc:                # wrong keys
                st.error(str(exc))
                return
            except ApiError as exc:                 # network / server problem
                st.error(f"Lookup failed: {exc}")
                return
            if found is None:                       # nothing came back
                st.session_state.pop("result", None)
                st.warning(f"No {mode.lower()} data found for that {key_type}.")
                return
            row = {key_type: search, **{label: found.get(label, "") for label, _ in specs}}  # 1-row table
            out = pd.DataFrame([row])
            st.session_state["result"] = {"single": True, "df": out, "raw": raw,
                                          "csv": out.to_csv(index=False).encode("utf-8-sig"),
                                          "file_name": f"lsq_{mode.lower()}_single.csv"}
    else:                                           # ── file upload ──
        upload = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="upload")
        if upload is None:                          # nothing uploaded yet
            render_result()
            return
        try:
            df = load_table(upload.name, upload.getvalue())  # cached read
        except Exception as exc:                    # unreadable file
            st.error(f"Could not read the file: {exc}")
            return
        if len(df) > MAX_ROWS:                      # enforce the per-file limit
            st.error(f"This file has {len(df):,} rows; the limit is {MAX_ROWS:,}. Please split it.")
            return
        st.caption(f"{len(df):,} rows · {len(df.columns)} columns")
        with st.expander("Preview", expanded=False):
            st.dataframe(df.head(10), use_container_width=True)
        plan = choose_key_plan(list(df.columns))    # [(key type, column), …]
        include_all = st.checkbox("Keep all original columns in the output", value=False)
        if st.button("🚀 Fetch fields", type="primary", disabled=not (creds and specs and plan)):
            client = LsqClient(creds, max_calls)      # fresh counters for this run
            try:
                values, matched, stats = run_batch(df, plan, specs, mode, pick_mode,
                                                   client, workers)  # the parallel part
            except AuthError as exc:                # stop early on bad keys
                st.error(f"{exc} Check your credentials file and host.")
                return
            out = build_output(df, plan, specs, values, matched, include_all)  # final table
            st.session_state["result"] = {"single": False, "df": out, "stats": stats,
                                          "csv": out.to_csv(index=False).encode("utf-8-sig"),
                                          "file_name": f"lsq_{mode.lower()}_fields.csv"}
    if not creds:                                   # remind the user what is missing
        st.caption("⬅️ Upload a credentials file in the sidebar to enable fetching.")
    render_result()                                 # always show the latest stored result


# ══════════════════════════════════════════════════════════════════════════════
#  7. App entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    st.set_page_config(page_title="LeadSquared Field Fetcher", page_icon="🔎", layout="wide")
    st.title("🔎LSQ Lens")

    with st.sidebar:                                # connection + performance settings
        st.header("🔐 Connection")
        cred_file = st.file_uploader("Credentials file (.json)", type=["json"], key="cred_file")
        creds = None                                # stays None until a valid file is uploaded
        if cred_file is not None:
            try:
                creds = parse_credentials(cred_file.getvalue())  # read keys at runtime
            except ValueError as exc:               # show what is wrong with the file
                st.error(str(exc))
        host = st.text_input("Host", value=creds.host if creds else DEFAULT_HOST)  # editable host
        if creds:
            creds.host = normalise_host(host)       # apply any edit
            st.success("Credentials loaded ✔")
        st.header("⚡ Speed")
        workers = st.slider("Parallel workers", 1, 30, 10, help="Threads making API calls at once.")
        max_calls = st.slider(f"Max API calls per {RATE_LIMIT_WINDOW:g} seconds", 1, RATE_LIMIT_CALLS, 28,
                              help=f"LeadSquared allows {RATE_LIMIT_CALLS} per {RATE_LIMIT_WINDOW:g} s. "
                                   "28 leaves a small safety margin; lower it if you still see failed lookups.")
        show_raw = st.checkbox("Show raw API response (single lead)", value=False)

    tab_fetch, tab_creds = st.tabs(["🔎 Fetch fields", "🔑 Create credentials file"])
    with tab_fetch:
        render_fetch_panel(creds, workers, max_calls, show_raw)
    with tab_creds:
        render_credentials_builder()


if __name__ == "__main__":                          # Streamlit runs the script as __main__
    main()
