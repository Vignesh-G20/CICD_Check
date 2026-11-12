# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4d2f381-f601-448f-b586-56bde2a6db83",
# META       "default_lakehouse_name": "Pinelabs_LH",
# META       "default_lakehouse_workspace_id": "883ac136-9d9d-429c-a3d4-50dd3e3df5f6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4d2f381-f601-448f-b586-56bde2a6db83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import base64
import json
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
import os
import re

# Create attachments folder
ATTACHMENTS_DIR = "/tmp/attachments"
os.makedirs(ATTACHMENTS_DIR, exist_ok=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
PROJECT_KEY = "PCF"
# Dynamic date range: last 15 days
TO_DATE = datetime.today().strftime('%Y-%m-%d')
FROM_DATE = (datetime.today() - timedelta(days=15)).strftime('%Y-%m-%d')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Clean local attachments folder
for file in os.listdir(ATTACHMENTS_DIR):
    file_path = os.path.join(ATTACHMENTS_DIR, file)
    if os.path.isfile(file_path):
        os.remove(file_path)
print("🧹 Local /tmp/attachments cleaned.")

# Clean Lakehouse attachments folder (Microsoft Fabric)
try:
    files = mssparkutils.fs.ls("Files/attachments/")
    for f in files:
        mssparkutils.fs.rm(f.path)
    print("🧹 Lakehouse attachments folder cleaned.")
except Exception as e:
    print(f"⚠️ Failed to clean Lakehouse folder: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

auth_str = f"{EMAIL}:{API_TOKEN}"
b64_auth = base64.b64encode(auth_str.encode()).decode()
headers = {
    "Authorization": f"Basic {b64_auth}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_filename_for_lakehouse(name):
    # Remove or replace all unsafe characters, including invisible Unicode spaces
    cleaned = re.sub(r'[^0-9a-zA-Z._-]', '_', name)
    # Remove consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    return cleaned

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def download_attachment(url, filename):
#     try:
#         full_path = os.path.join(ATTACHMENTS_DIR, filename)
#         resp = requests.get(url, headers=headers, stream=True)
#         resp.raise_for_status()
#         with open(full_path, "wb") as f:
#             for chunk in resp.iter_content(chunk_size=8192):
#                 f.write(chunk)
#         return full_path
#     except Exception as e:
#         print(f"❌ Failed to download {filename}: {e}")
#         return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_all_issues():
    all_issues = []
    next_page_token = None
    max_results = 2000
    
    jql = f'project = {PROJECT_KEY} AND created >= "{FROM_DATE}" AND created <= "{TO_DATE}" ORDER BY created ASC'
    fields = [
        "summary", "description", "customfield_10140", "customfield_10243", "customfield_10244", "customfield_10245", 
    "customfield_10247", "customfield_11293", "attachment", "customfield_11127", "subtasks", "issuelinks", "comment",
    "statusCategory", "resolution", "assignee", "reporter", "customfield_10044", "customfield_10045", "customfield_10249", 
    "customfield_10058", "customfield_10050", "customfield_10238", "customfield_10239", "customfield_10230",
    "customfield_10101", "customfield_10253", "customfield_10240", "customfield_10043", "customfield_11687", 
    "customfield_10246","customfield_10234", "customfield_11292", "customfield_11588", "customfield_11589", 
    "customfield_11594", "customfield_11590", "customfield_11591", "priority", "created", "updated", "resolutiondate"
    ]
    url = f"https://{JIRA_DOMAIN}/rest/api/3/search/jql"
    
    while True:
        payload = {"jql": jql, "maxResults": max_results, "fields": fields}
        if next_page_token:
            payload['nextPageToken'] = next_page_token
            
        resp = requests.post(url, headers=headers, json=payload, auth=(EMAIL, API_TOKEN))
        resp.raise_for_status()
        data = resp.json()
        issues = data.get("issues", [])
        if not issues:
            break
        all_issues.extend(issues)

        next_page_token = data.get("nextPageToken")
        if next_page_token is None:
            print(f"✅ Fetched {len(all_issues)} issues in total.")
            break
        print(f"Fetched {len(all_issues)} issues so far...")
    
    return all_issues


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_downloaded_files = set()
failed_attachments = []

def parse_issue(issue):
    f = issue["fields"]
    k = issue["key"]

    # --- Subtasks ---
    subtasks = ', '.join([st.get('key') for st in f.get('subtasks', [])]) or None
    
    def extract_text_from_json(data):
        """
        Extracts text from Jira 'doc' JSON.
        - Normal paragraphs and inline content are returned as strings.
        - Tables are extracted as structured dicts with header mapping.
        - Table boundaries are clearly marked with "TABLE START" / "TABLE END".
        """
        if not data:
            return None

        result = []

        def extract_content(content_list):
            for item in content_list:
                item_type = item.get("type")

                # Plain text
                if item_type == "text":
                    result.append(item.get("text", "").strip())

                # Mentions like @user
                elif item_type == "mention":
                    result.append(item.get("attrs", {}).get("text", "").strip())

                # InlineCard / URLs
                elif item_type == "inlineCard":
                    result.append(item.get("attrs", {}).get("url", "").strip())

                # Paragraphs
                elif item_type == "paragraph":
                    extract_content(item.get("content", []))

                # Table
                elif item_type == "table":
                    result.append("TABLE START")
                    rows = item.get("content", [])
                    if rows:
                        # First row is headers
                        header_row = rows[0].get("content", [])
                        headers = [extract_cell_text(cell) for cell in header_row]

                        # Initialize structured table dict
                        table_dict = {h: {} for h in headers}

                        # Process remaining rows
                        for r_idx, row in enumerate(rows[1:], start=1):
                            cells = row.get("content", [])
                            for c_idx, cell in enumerate(cells):
                                header = headers[c_idx] if c_idx < len(headers) else f"Column_{c_idx}"
                                table_dict[header][r_idx] = extract_cell_text(cell)
                        
                        # Append table dict as string representation
                        result.append(str(table_dict))
                    result.append("TABLE END")

                # Table row
                elif item_type == "tableRow":
                    extract_content(item.get("content", []))

                # Table cell
                elif item_type == "tableCell":
                    extract_content(item.get("content", []))

        def extract_cell_text(cell):
            """
            Extracts all text inside a table cell recursively.
            """
            texts = []

            def recurse(c):
                for i in c.get("content", []):
                    t_type = i.get("type")
                    if t_type == "text":
                        texts.append(i.get("text", "").strip())
                    elif t_type in ["paragraph", "tableCell", "tableRow"]:
                        recurse(i)
                    elif t_type == "inlineCard":
                        texts.append(i.get("attrs", {}).get("url", "").strip())
                    elif t_type == "mention":
                        texts.append(i.get("attrs", {}).get("text", "").strip())

            recurse(cell)
            return " ".join(texts)

        extract_content(data.get("content", []))
        return "\n".join([r for r in result if r.strip()])

    # --- Business Product & Systems Impacted, Change_Prerequisites_Checklist, Multiple Approvals---
    def extract_business_product_text(data, field):
        text = ";".join(i.get(field) for i in data) if data else None
        return text

    def extract_testers_text(data):
        if not data:
            return None
        testers = []
        for tester in data:
            name = tester.get("displayName", None)
            emailAddress = tester.get("emailAddress", None)
            testers.append(f"{name} - {emailAddress}")
        return ";".join(testers)

    # ---Issue links ---
    issuelink_field = f.get("issuelinks", None)
    issuelinks = ""
    issuelinks_lines = []

    if not issuelink_field:
        issuelinks = None
    else:
        for link in issuelink_field:
            issue = link.get("inwardIssue") or link.get("outwardIssue")
            if issue:
                key = issue.get("key", "Unknown")
                summary = issue.get("fields", {}).get("summary", "No Summary")
                issuelinks_lines.append(f"{key} - {summary}")
        
        issuelinks = "\n".join(issuelinks_lines)

    def convert_datetime(date):
        if not date:
            return None
        created = date[:19]
        dt = datetime.strptime(created, '%Y-%m-%dT%H:%M:%S')
        return dt.astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')

    def get_parent_Ticket_ID(data):   
        if not data:
            return ""
        
        key = data.get("key")
        return str(key) if key is not None else ""

    def extract_approvers_text(data):
        if not data:
            return ""

        formatted_users = []
        for user in data:
            if user:  # skip None entries
                name = user.get("displayName") or ""
                email = user.get("emailAddress") or ""
                formatted_users.append(f"{name} - {email}")

        return ";".join(formatted_users)

    # --- Attachments: Download & sanitize filenames ---
    attachment_paths = []
    for a in f.get("attachment", []):
        orig_name = a['filename']
        safe_name = clean_filename_for_lakehouse(f"{k}_{orig_name}")
        file_url = a.get("content")
        local_path = os.path.join(ATTACHMENTS_DIR, safe_name)
        try:
            resp = requests.get(file_url, headers=headers, stream=True)
            resp.raise_for_status()
            with open(local_path, "wb") as f_local:
                for chunk in resp.iter_content(chunk_size=8192):
                    f_local.write(chunk)
            all_downloaded_files.add(safe_name)
            attachment_paths.append(local_path)
        except Exception as e:
            # print(f"❌ Failed to download {safe_name}: {e}")
            failed_attachments.append({"Issue Key": k, "Attachment": orig_name})
            attachment_paths.append(f"[Failed] {orig_name}")

    # --- Return parsed issue dict ---
    return {
        "Ticket_ID": k,
        # "Parent_Ticket_ID": get_parent_Ticket_ID(f.get("parent", None)),
        "Parent_Ticket_ID": get_parent_Ticket_ID(f.get("parent")),
        "Summary": f.get("summary", None),
        "Description": str(f.get("description", None)),
        "Business_Justification":extract_text_from_json(f.get("customfield_10140", None)),
        "Rollback_Script": extract_text_from_json(f.get("customfield_10243", None)),
        "Testplan_and_TestResults": extract_text_from_json(f.get("customfield_10244", None)),
        "Subtask_Execution_Sequence": extract_text_from_json(f.get("customfield_10245", None)),
        "Validation_Monitoring_Steps": extract_text_from_json(f.get("customfield_10247", None)),
        "PCF_Description": extract_text_from_json(f.get("customfield_11293", None)),
        "Attachments": '; '.join(attachment_paths) if attachment_paths else None,
        "Comment_on_Post_Production_Sanity": extract_text_from_json(f.get("customfield_11127", None)),
        "Subtasks": subtasks,
        "Issue_Links": issuelinks,
        "Comments": str(f.get("comment", {}).get("comments", [])),
        "Status": (f.get("statusCategory") or {}).get("name", None),
        "Resolved_or_Not": (f.get("resolution") or {}).get("name", None),
        "Assignee": (f.get("assignee") or {}).get("displayName", None),
        "Assignee_mailID": (f.get("assignee") or {}).get("emailAddress", None),
        "Reporter": (f.get("reporter") or {}).get("displayName", None),
        "Reporter_mailID": (f.get("reporter") or {}).get("emailAddress", None),
        "Business_Product": extract_business_product_text(f.get("customfield_10044", None), "value"),
        "Systems_Impacted": extract_business_product_text(f.get("customfield_10045", None), "value"),
        "MD5_Checksum_or_CommitID": f.get("customfield_10249", None),
        "Approvers": extract_approvers_text(f.get("customfield_10058", [])),
        "Testers": extract_testers_text(f.get("customfield_10050", None)),
        "Change_Request_Manager": (f.get("customfield_10238") or {}).get("displayName", None),
        "Change_Request_Manager_mailID": (f.get("customfield_10238") or {}).get("emailAddress", None),
        "Change_Prerequisites_Checklist": extract_business_product_text(f.get("customfield_10239", None), "value"),
        "Planned_Start_Date": convert_datetime(f.get("customfield_10230", None)),
        "Planned_End_Date": convert_datetime(f.get("customfield_10101", None)),
        "UAT_or_Test_IssueID": f.get("customfield_10253", None),
        "QA_or_UAT_Required": (f.get("customfield_10240") or {}).get("value", None),
        "Assigned_Team": (f.get("customfield_10043") or {}).get("name", None),
        "Emergency_Change_Justification": f.get("customfield_11687", None),
        "Source_Code_Diff_or_Pull_Request_or_PR_Corresponding_To_The_Change": f.get("customfield_10246", None),
        "Multiple_Approvals": extract_business_product_text(f.get("customfield_10234", None),"name"),
        "Deployer_or_Change_Implementer": (f.get("customfield_11292") or {}).get("displayName", None),
        "Deployer_or_Change_Implementer_mailID": (f.get("customfield_11292") or {}).get("emailAddress", None),
        "Waiting_Approval_Nrml": convert_datetime(f.get("customfield_11588", None)),
        "Waiting_Implem_Nrml": convert_datetime(f.get("customfield_11589", None)),
        "Approval_Difference_Hours_Nrml": f.get("customfield_11594", None),
        "Waiting_Approval_Emergency": convert_datetime(f.get("customfield_11590", None)),
        "Waiting_Implem_Emergency": convert_datetime(f.get("customfield_11591", None)),
        "Priority": (f.get("priority") or {}).get("name", None),
        "Created_DateTime": convert_datetime(f.get("created", None)),
        "Last_Updated_DateTime": convert_datetime(f.get("updated", None)),
        "Resolved_DateTime": convert_datetime(f.get("resolutiondate", None))
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
import pandas as pd
import os

print(f"Fetching issues from {FROM_DATE} to {TO_DATE}...")

# 1️⃣ Fetch and parse issues (source)
issues = fetch_all_issues()
parsed_data = [parse_issue(i) for i in issues]

# 2️⃣ Convert to pandas DataFrame
df = pd.DataFrame(parsed_data)
print(f"✅ Fetched {len(df)} issues.")

# 3️⃣ Convert pandas DF to Spark DF
spark_df = spark.createDataFrame(df)

# 4️⃣ Save directly as Delta table in lakehouse
delta_table_name = "jira_tickets"
spark_df.write.format("delta").mode("overwrite").saveAsTable(delta_table_name)
print(f"✅ Saved {len(df)} issues directly to Delta table '{delta_table_name}'")

# 5️⃣ Optional: still save CSV for archival (if you want)
csv_path = "/tmp/jira_tickets.csv"
df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
lakehouse_csv_name = clean_filename_for_lakehouse("jira_tickets.csv")
mssparkutils.fs.cp(f"file:{csv_path}", f"Files/{lakehouse_csv_name}")
print(f"✅ Also saved CSV to Files/{lakehouse_csv_name}")

# 6️⃣ Upload attachments
for fname in all_downloaded_files:
    full_path = os.path.join(ATTACHMENTS_DIR, fname)
    if os.path.isfile(full_path):
        mssparkutils.fs.cp(f"file:{full_path}", f"Files/attachments/{fname}")

print(f"✅ Uploaded {len(all_downloaded_files)} attachments to Files/attachments/")

# 7️⃣ Save failed attachments CSV (if any)
if failed_attachments:
    df_failed = pd.DataFrame(failed_attachments)
    failed_csv_path = "/tmp/failed_attachments.csv"
    df_failed.to_csv(failed_csv_path, index=False)
    safe_failed_csv = clean_filename_for_lakehouse("failed_attachments.csv")
    mssparkutils.fs.cp(f"file:{failed_csv_path}", f"Files/{safe_failed_csv}")
    print(f"⚠️ Some attachments failed. Details saved to Files/{safe_failed_csv}")
else:
    print("✅ All attachments downloaded and uploaded successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_text_from_description(desc_json):
    """
    Extracts readable text and structured tables (as dicts) from Jira's description JSON.
    Safe to call with None.
    """
    if not desc_json or not isinstance(desc_json, dict):
        return ""

    result = []

    def extract_cell_text_description(cell):
        urls, texts = [], []

        def recurse(c):
            for i in c.get("content", []):
                t_type = i.get("type")

                # plain text (may have marks with links)
                if t_type == "text":
                    text_val = i.get("text", "").strip()
                    marks = i.get("marks", []) or []
                    link_url = None
                    for m in marks:
                        if m.get("type") == "link":
                            link_url = m.get("attrs", {}).get("href", "").strip()
                    if link_url:
                        urls.append(link_url)
                    if text_val:
                        texts.append(text_val)

                elif t_type in ("heading", "paragraph", "tableCell", "tableRow", "listItem", "bulletList", "orderedList"):
                    recurse(i)

                elif t_type == "inlineCard":
                    url = i.get("attrs", {}).get("url", "").strip()
                    if url:
                        urls.append(url)

                elif t_type == "mention":
                    mention_text = i.get("attrs", {}).get("text", "").strip()
                    if mention_text:
                        texts.append(mention_text)

                elif t_type == "hardBreak":
                    texts.append("\n")

                elif t_type == "emoji":
                    texts.append(i.get("attrs", {}).get("shortName", "").strip())

                elif t_type == "status":
                    texts.append(i.get("attrs", {}).get("text", "").strip())

        recurse(cell)
        cleaned_texts = [t for t in ((" ".join(texts)).splitlines()) if t.strip()]
        # if urls and cleaned_texts:
        #     return f"URL: {urls[0]}\nTEXT: {' '.join(cleaned_texts)}"
        # elif urls:
        if urls:
            return "\t".join([t.strip() for t in urls if t.strip()])
        elif texts:
            return " ".join([t.strip() for t in texts if t.strip()])
        else:
            return ""

    # --- Process main content ---
    for item in desc_json.get("content", []):
        item_type = item.get("type")

        # Headings
        if item_type == "heading":
            heading_text = [c.get("text", "") for c in item.get("content", []) if c.get("type") == "text"]
            if heading_text:
                result.append(" ".join(heading_text))

        # Paragraphs
        elif item_type == "paragraph":
            paragraph_text = []
            for c in item.get("content", []):
                ctype = c.get("type")
                if ctype == "text":
                    paragraph_text.append(c.get("text", ""))
                    cmark = c.get("marks", [])
                    if cmark != []:
                        for m in cmark:
                            cmarktype = m.get("type")
                            if cmarktype == "link":
                                paragraph_text.append(f"\n{m.get('attrs', {}).get('href', '')}")
                elif ctype == "mention":
                    paragraph_text.append(c.get("attrs", {}).get("text", ""))
                elif ctype == "inlineCard":
                    paragraph_text.append(c.get("attrs", {}).get("url", ""))
                elif ctype == "hardBreak":
                    paragraph_text.append("\n")
            if paragraph_text:
                result.append(" ".join(paragraph_text))

        # Lists
        elif item_type in ("bulletList", "orderedList"):
            for list_item in item.get("content", []):
                for p in list_item.get("content", []):
                    texts = []
                    for c in p.get("content", []):
                        if c.get("type") == "text":
                            texts.append(c.get("text", ""))
                        elif c.get("type") == "mention":
                            texts.append(c.get("attrs", {}).get("text", ""))
                    if texts:
                        result.append(" ".join(texts))

        # Tables
        elif item_type == "table":
            rows = item.get("content", [])
            if not rows:
                continue

            # headers
            header_cells = rows[0].get("content", [])
            headers = [extract_cell_text_description(cell).strip() or f"Column_{i}" for i, cell in enumerate(header_cells, start=1)]

            # data rows
            data_rows = []
            for row in rows[1:]:
                cells = row.get("content", [])
                row_vals = [extract_cell_text_description(cell).strip() for cell in cells]
                while len(row_vals) < len(headers):
                    row_vals.append("")
                data_rows.append(row_vals)

            # build dict
            table_dict = {h: {} for h in headers}
            for idx, row_vals in enumerate(data_rows, start=1):
                for h, val in zip(headers, row_vals):
                    table_dict[h][idx] = val

            result.append("TABLE START")
            result.append(str(table_dict))
            result.append("TABLE END")

    return "\n".join(result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
