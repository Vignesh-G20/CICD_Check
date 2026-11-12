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
import json

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
PROJECT_KEY = "PCF"

# JIRA_DOMAIN = "your-domain.atlassian.net"
ISSUE_KEY = "PCF-47839"  # 👈 Replace with your ticket key
# API_TOKEN = "your_api_token"
# EMAIL = "your_email@example.com"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

resp = requests.get(
    f"https://{JIRA_DOMAIN}/rest/api/3/issue/{ISSUE_KEY}?expand=changelog",
    headers=headers,
    auth=(EMAIL, API_TOKEN)
)

resp.raise_for_status()
issue = resp.json()

# Print all fields in a readable format
print(json.dumps(issue, indent=2))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import base64
import json
import re
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
import os


# Configuration
JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "srinath.babu@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0DYsuyP-kcK6kZzf5adBn_qnZ0FFT6U7MZGFEIstd15WRSEOM78aBktzFvfGJ8J8fs4QkWb6JSpTD9snYAzR-KKEDF0NuZoorpYDaB6OBFa6fHfj0LYfWkAYchUIr26BT75vNvNpCPhpvajI1HaNiV59MFYiEJBjVSSOnNydFO8k=1E17D3BB"
PROJECT_KEY = "DC" 

# Dynamic date range: last 45 days
TO_DATE = datetime.today().strftime('%Y-%m-%d')
FROM_DATE = (datetime.today() - timedelta(days=45)).strftime('%Y-%m-%d')
ATTACHMENTS_DIR = "/tmp/attachments"
os.makedirs(ATTACHMENTS_DIR, exist_ok=True)

# === Clean local attachments folder ===
try:
    for file in os.listdir(ATTACHMENTS_DIR):
        file_path = os.path.join(ATTACHMENTS_DIR, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print("🧹 Cleaned local /tmp/attachments folder.")
except Exception as e:
    print(f"⚠️ Failed to clean local attachments folder: {e}")

# === Clean Lakehouse attachments folder ===
try:
    files = mssparkutils.fs.ls("Files/attachments/")
    for f in files:
        mssparkutils.fs.rm(f.path)
    print("🧹 Cleaned Files/attachments/ before upload.")
except Exception as e:
    print(f"⚠️ Failed to clean Lakehouse attachments folder: {e}")

# === Auth headers ===
auth_str = f"{EMAIL}:{API_TOKEN}"
b64_auth = base64.b64encode(auth_str.encode()).decode()
headers = {
    "Authorization": f"Basic {b64_auth}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# === Helpers ===
def clean_filename(name):
    return re.sub(r'[<>:"/\\|?*\[\]() ]', '_', name)

def get_custom_field_value(field):
    if isinstance(field, dict) and "value" in field:
        return field["value"]
    elif isinstance(field, str):
        return field
    elif field is not None:
        return str(field)
    return "Not Set"

def download_attachment(url, filename):
    try:
        full_path = os.path.join(ATTACHMENTS_DIR, filename)
        resp = requests.get(url, headers=headers, stream=True)
        resp.raise_for_status()
        with open(full_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return full_path
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        return None

# === Get comments ===
def get_comments(issue_key):
    url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}/comment"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return []
    return resp.json().get("comments", [])

# === Fetch all issues ===
def fetch_all_issues():
    all_issues = []
    next_page_token = None
    max_results = 2000
    # all_issues = []
    # start_at = 0
    
    jql = f'project = {PROJECT_KEY} AND created >= "{FROM_DATE}" AND created <= "{TO_DATE}" ORDER BY created ASC'
    fields = [
                "summary", "description", "status", "assignee", "reporter",
                "priority", "labels", "created", "updated", "duedate", "attachment",
                "customfield_10230", "customfield_10101", "customfield_10679",
                "customfield_10938", "customfield_10683"
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

# === Process issues ===
all_downloaded_files = set()

def parse_issue(issue):
    f = issue["fields"]
    k = issue["key"]

    def get_text(doc):
        if isinstance(doc, dict):
            if doc.get("type") == "text" and "text" in doc:
                return doc["text"]
            return ''.join([get_text(c) for c in doc.get("content", [])])
        elif isinstance(doc, list):
            return ''.join([get_text(i) for i in doc])
        return ''

    # Comments
    comment_strs = []
    for c in get_comments(k):
        try:
            author = c['author']['displayName']
            created = c['created']
            dt = datetime.strptime(created[:19], '%Y-%m-%dT%H:%M:%S')
            local_dt = dt.astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')
            text = get_text(c.get('body', {}).get('content', [])) or 'No text'
            comment_strs.append(f"{local_dt} - {author}: {text}")
        except:
            continue

    # DML Script
    dml_script = f.get("customfield_10938")
    if isinstance(dml_script, dict):
        dml_script = get_text(dml_script.get("content", [])) or dml_script.get("value", "Not Set")
    elif isinstance(dml_script, str):
        dml_script = dml_script
    else:
        dml_script = "Not Set"

    # Attachments: Download and list filenames
    attachment_paths = []
    for a in f.get("attachment", []):
        raw_name = clean_filename(a["filename"])
        file_url = a.get("content")
        final_name = f"{k}_{raw_name}"
        downloaded = download_attachment(file_url, final_name)
        if downloaded:
            all_downloaded_files.add(final_name)
        attachment_paths.append(downloaded or f"[Failed] {raw_name}")

    return {
        "Issue Key": issue.get("key", ""),
        "Issue ID": issue.get('id', 'Unknown'),
        "Summary": f.get("summary", ""),
        "Description": f.get("description", "No summary"),
        "Status": f.get("status", {}).get("name", ""),
        "Assignee": f.get("assignee", {}).get("displayName", "Unassigned") if f.get("assignee") else "Unassigned",
        "Reporter": f.get("reporter", {}).get("displayName", ""),
        "Priority": f.get("priority", {}).get("name", ""),
        "Labels": ', '.join(f.get("labels", [])),
        "Created Date": f.get("created", ""),
        "Updated Date": f.get("updated", ""),
        "Due Date": f.get("duedate", ""),
        "Planned Start Date": f.get("customfield_10230", ""),
        "Planned End Date": f.get("customfield_10101", ""),
        "DML Operation": get_custom_field_value(f.get("customfield_10679")),
        "DML Script": dml_script,
        "Table Name": get_custom_field_value(f.get("customfield_10683")),
        "Attachments": '; '.join(attachment_paths) if attachment_paths else "No attachments",
        "Comments": '; '.join(comment_strs) if comment_strs else "No Comments"
    }

# === MAIN EXECUTION ===
print(f"Fetching issues from {FROM_DATE} to {TO_DATE}...")
issues = fetch_all_issues()
parsed_data = [parse_issue(i) for i in issues]
df = pd.DataFrame(parsed_data)

# Save to Lakehouse Files as CSV
csv_path = "/tmp/jira_tickets.csv"
df.to_csv(csv_path, index=False)
mssparkutils.fs.cp("file:/tmp/jira_tickets.csv", "Files/jira_tickets.csv")
print(f"✅ Saved {len(df)} issues to Files/jira_tickets.csv")

# Copy ONLY current session's downloaded attachments to Lakehouse Files
try:
    for fname in all_downloaded_files:
        full_path = os.path.abspath(os.path.join(ATTACHMENTS_DIR, fname))
        if os.path.isfile(full_path):
            mssparkutils.fs.cp(f"file:{full_path}", f"Files/attachments/{clean_filename(fname)}")
    print(f"✅ Copied session attachments to Lakehouse: Files/attachments/")
except Exception as e:
    print(f"❌ Failed to copy attachments: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit, col, when

# Load the tickets table from your Lakehouse
tickets_df = spark.read.table("tickets")

# Add the status column with a default False value
tickets_df = tickets_df.withColumn("status", lit(False))

# Write back to the Lakehouse (overwrite or merge, depending on your setup)
tickets_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tickets")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import requests

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# ----------------------------
# 1️⃣ Load the tickets table
# ----------------------------
tickets_df = spark.read.table("tickets")

# Ensure Ticket_ID is uppercase to match Jira issue keys
# tickets_df = tickets_df.withColumnRenamed("ticket_id", "Ticket_ID")

# ----------------------------
# 2️⃣ Jira configuration
# ----------------------------

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
PROJECT_KEY = "PCF"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def get_jira_status(issue_key):
    """Returns True if Jira description exists, False if null."""
    try:
        url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}?fields=description"
        resp = requests.get(url, headers=headers, auth=(EMAIL, API_TOKEN))
        if resp.status_code != 200:
            print(f"Failed to fetch {issue_key}: {resp.status_code}")
            return None
        data = resp.json()
        desc = data["fields"].get("description")
        return desc is not None
    except Exception as e:
        print(f"Error fetching {issue_key}: {e}")
        return None

# ----------------------------
# 3️⃣ Fetch description status for each ticket
# ----------------------------
updated_rows = []
for row in tickets_df.collect():
    issue_key = row["Ticket_ID"]
    status = get_jira_status(issue_key)
    updated_rows.append((issue_key, status))

# Create a new DataFrame with Ticket_ID and status
from pyspark.sql import Row
updated_df = spark.createDataFrame([Row(Ticket_ID=i, status=s) for i, s in updated_rows])

# ----------------------------
# 4️⃣ Merge with the existing tickets table
# ----------------------------
# Drop old status column if it already exists
if "status" in tickets_df.columns:
    tickets_df = tickets_df.drop("status")

# Join updated status values
final_df = tickets_df.join(updated_df, "Ticket_ID", "left")

# ----------------------------
# 5️⃣ Save back to Lakehouse
# ----------------------------
final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tickets")

print("✅ Tickets table updated successfully with 'status' column.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from tickets 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
PROJECT_KEY = "PCF"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def get_jira_status(issue_key):
    """Returns True if Jira description exists, False if null or missing."""
    url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}?fields=description"
    resp = requests.get(url, headers=headers, auth=(EMAIL, API_TOKEN))
    
    if resp.status_code != 200:
        print(f"Failed to fetch {issue_key}: {resp.status_code}")
        return None  # Or handle error
    
    data = resp.json()
    desc = data["fields"].get("description")
    return desc is not None

# Apply to all tickets
updated_rows = []
for row in tickets_df.collect():
    issue_key = row["Ticket_ID"]
    status = get_jira_status(issue_key)
    updated_rows.append((issue_key, status))

# Create new DataFrame with updated status
from pyspark.sql import Row
updated_df = spark.createDataFrame([Row(ticket_id=i, status=s) for i, s in updated_rows])

# Write back (join/merge to update original table)
final_df = tickets_df.join(updated_df, "Ticket_ID", "left").select(
    tickets_df["*"],
    updated_df["status"]
)

# Save the updated table
final_df.write.mode("overwrite").saveAsTable("tickets")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
PROJECT_KEY = "PCF"

# JIRA_DOMAIN = "your-domain.atlassian.net"
ISSUE_KEY = "PCF-46125"  # 👈 Replace with your ticket key
# API_TOKEN = "your_api_token"
# EMAIL = "your_email@example.com"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

resp = requests.get(
    f"https://{JIRA_DOMAIN}/rest/api/3/issue/{ISSUE_KEY}?fields=*all",
    headers=headers,
    auth=(EMAIL, API_TOKEN)
)

resp.raise_for_status()
issue = resp.json()

# Print all fields in a readable format
# print(json.dumps(issue["fields"], indent=2))

f=issue["fields"]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

f = {"parent":{
  "id": "921503",
  "key": "PCF-46242",
  "self": "https://pinelabsgroups.atlassian.net/rest/api/3/issue/921503",
  "fields": {
    "summary": "PCUI || Instore Deployment 8th October 2025",
    "status": {
      "self": "https://pinelabsgroups.atlassian.net/rest/api/3/status/6",
      "description": "Epic and all child issues are complete, with review/acceptance confirmed",
      "iconUrl": "https://pinelabsgroups.atlassian.net/images/icons/statuses/closed.png",
      "name": "Closed",
      "id": "6",
      "statusCategory": {
        "self": "https://pinelabsgroups.atlassian.net/rest/api/3/statuscategory/3",
        "id": 3,
        "key": "done",
        "colorName": "green",
        "name": "Done"
      }
    },
    "priority": {
      "self": "https://pinelabsgroups.atlassian.net/rest/api/3/priority/10007",
      "iconUrl": "https://pinelabsgroups.atlassian.net/images/icons/priorities/medium.svg",
      "name": "Medium",
      "id": "10007"
    },
    "issuetype": {
      "self": "https://pinelabsgroups.atlassian.net/rest/api/3/issuetype/10036",
      "id": "10036",
      "description": "Created by Jira Service Management. ",
      "iconUrl": "https://pinelabsgroups.atlassian.net/rest/api/2/universal_avatar/view/type/issuetype/avatar/10300?size=medium",
      "name": "Normal Change",
      "subtask": "false",
      "avatarId": 10300,
      "hierarchyLevel": 0
    }
  }
}}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def get_parent_Ticket_ID(data):        
#         # if not data:
#         #     return None
        
#         # issue_key = str(data.get("key")) if data.get("key") else None
#         # return issue_key
#         if not data:
#             return ""
        
#         key = data.get("key")
#         return str(key) if key is not None else ""

def get_parent_Ticket_ID(data):
    if not isinstance(data, dict):
        return None
    return str(data.get("key") or None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(get_parent_Ticket_ID(f.get("parent")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("jira_tickets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("check_jira_tickets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.table("check_jira_tickets")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mainPageFields = [
    "summary", "description", "customfield_10140", "customfield_10243", "customfield_10244", "customfield_10245", 
    "customfield_10247", "customfield_11293", "attachment", "customfield_11127", "subtasks", "issuelinks", "comment"
]

rightPageFields = [
    "statusCategory", "resolution", "assignee", "reporter", "customfield_10044", "customfield_10045", "customfield_10249", 
    "customfield_10058", "customfield_10050", "customfield_10238", "customfield_10239", "customfield_10230",
    "customfield_10101", "customfield_10253", "customfield_10240", "customfield_10043", "customfield_11687", 
    "customfield_10246","customfield_10234", "customfield_11292", "customfield_11588", "customfield_11589", 
    "customfield_11594", "customfield_11590", "customfield_11591", "priority", "created", "updated", "resolutiondate"
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

def extract_text_from_node(node, prefer_link=False):
    """Recursively extract text from a node with optional link preference."""
    node_type = node.get("type")

    # Direct text
    if node_type == "text":
        # Check if it has link marks
        if "marks" in node:
            for mark in node["marks"]:
                if mark.get("type") == "link" and prefer_link:
                    href = mark["attrs"].get("href")
                    if href:
                        return href.strip()
        return node.get("text", "").strip()

    # Mention (@name)
    if node_type == "mention":
        return node["attrs"].get("text", "").strip()

    # Inline Card (links)
    if node_type == "inlineCard":
        return node["attrs"].get("url", "").strip()

    # Line break
    if node_type == "hardBreak":
        return "\n"

    # Paragraph
    if node_type == "paragraph" and "content" in node:
        return "".join(extract_text_from_node(c, prefer_link) for c in node["content"])

    # Ordered list / bullet list
    if node_type in ["orderedList", "bulletList"]:
        if "content" in node:
            return "\n".join(
                extract_text_from_node(item, prefer_link) for item in node["content"]
            )

    # List item
    if node_type == "listItem" and "content" in node:
        return "".join(
            extract_text_from_node(c, prefer_link) for c in node["content"]
        )

    return ""


def extract_text_blocks(adf):
    """Extract plain text content but exclude tables."""
    text_blocks = []

    def traverse(content):
        for item in content:
            # Skip tables
            if item.get("type") == "table":
                continue

            # Normal text / list
            if item.get("type") in ["paragraph", "listItem", "orderedList", "bulletList"]:
                text = extract_text_from_node(item)
                if text.strip():
                    text_blocks.append(text.strip())

            if "content" in item and item.get("type") != "table":
                traverse(item["content"])

    traverse(adf.get("content", []))
    return "\n".join(text_blocks).strip() if text_blocks else ""


def extract_tables(adf):
    """Extract tables with support for lists and links inside cells."""
    tables = []

    def traverse(content):
        for item in content:
            if item.get("type") == "table":
                headers = []
                rows_data = []

                for idx, row in enumerate(item.get("content", [])):
                    cells = []
                    for cell in row.get("content", []):
                        cell_text = ""
                        if "content" in cell:
                            cell_text = "".join(
                                extract_text_from_node(c, prefer_link=True)
                                for c in cell["content"]
                            )
                        cells.append(cell_text.strip())
                    if idx == 0:
                        headers = cells
                    else:
                        rows_data.append(cells)
                tables.append({"headers": headers, "rows": rows_data})

            elif "content" in item:
                traverse(item["content"])

    traverse(adf.get("content", []))
    return tables


def combine_text_and_tables(adf):
    """Combine text and tables into a final DataFrame."""
    merged_text = extract_text_blocks(adf)
    tables = extract_tables(adf)
    dfs = []

    # ✅ Add table dataframes
    for t in tables:
        headers = t["headers"]
        df = pd.DataFrame(t["rows"], columns=headers)
        dfs.append(df)

    # ✅ Add text as single row
    if merged_text:
        text_df = pd.DataFrame([{"Description": merged_text}])
        dfs.append(text_df)

    # ✅ Merge with column alignment
    if dfs and len(dfs) > 1:
        final_df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
    elif dfs:
        final_df = dfs[0]
    else:
        final_df = pd.DataFrame(columns=["Description"])

    return final_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

description_json = issue["fields"]["description"]
final_df = combine_text_and_tables(description_json)
display(final_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import pandas as pd

JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"

# List of Jira issue keys you want to fetch
ISSUE_KEYS = ["PCF-46169", "PCF-46125", "PCF-45993"]  # Add more keys as needed

# Fields you want to fetch (for now 'description', later you can add more)
FIELDS = ["description"]  # Add more fields like "summary", "assignee", "status" etc.

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def fetch_jira_issue(issue_key):
    url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}?fields={','.join(FIELDS)}"
    resp = requests.get(url, headers=headers, auth=(EMAIL, API_TOKEN))
    resp.raise_for_status()
    return resp.json()

# Store all descriptions
all_issues_data = []

for key in ISSUE_KEYS:
    issue = fetch_jira_issue(key)
    fields = issue.get("fields", {})
    
    # Extract description text (if it's empty, store "")
    description_content = fields.get("description")
    
    # Optional: If description is stored as rich text (ADF JSON), store JSON itself
    # Later you can process it using your existing combine_text_and_tables() function
    all_issues_data.append({
        "IssueKey": key,
        "Description": description_content  # Store raw JSON for processing later
    })

# Convert to DataFrame
df = pd.DataFrame(all_issues_data)

# Example: Merge all descriptions into a single text (if needed)
merged_descriptions = " \n\n ".join(
    [json.dumps(d["Description"]) if isinstance(d["Description"], dict) else str(d["Description"])
     for d in all_issues_data]
)

display(df)
# print(merged_descriptions)  # merged text of all issues


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import pandas as pd

# ------------------ JIRA CONFIG ------------------
JIRA_DOMAIN = "pinelabsgroups.atlassian.net"
EMAIL = "vignesh.g2@pinelabs.com"
API_TOKEN = "ATATT3xFfGF0h_VgPVRc_gOidFwcf1oDRj9b03NA9U4bYRgJBBtnyE2aMnyzCdj0F8uhv648kA15OgTtPkngXgaerx3LSvxIdr7KxzT8quS7wIe1BUELA6a6NK5hXI35JQZZAhx3mfUcH3xSzP6zU19GQEUvxG-6oKrOruZRf_eofiY9NXOXKvk=69AA7B2B"
ISSUE_KEYS = ["PCF-46169", "PCF-46125", "PCF-45993"]  # Add more keys as needed

FIELDS = ["description"]  # Add more fields if needed

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def fetch_jira_issue(issue_key):
    url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}?fields={','.join(FIELDS)}"
    resp = requests.get(url, headers=headers, auth=(EMAIL, API_TOKEN))
    resp.raise_for_status()
    return resp.json()

# ------------------ ADF PROCESSING ------------------
def extract_text_from_node(node, prefer_link=False):
    """Extract plain text from nodes; prefer link if available."""
    t = node.get("type")
    if t == "text":
        # Check for link marks
        if "marks" in node:
            for mark in node["marks"]:
                if mark.get("type") == "link" and prefer_link:
                    return mark["attrs"].get("href", "").strip()
        return node.get("text", "").strip()
    if t == "mention":
        return node["attrs"].get("text", "").strip()
    if t == "inlineCard":
        return node["attrs"].get("url", "").strip()
    if t == "hardBreak":
        return "\n"
    if t in ["paragraph", "listItem"] and "content" in node:
        return "".join(extract_text_from_node(c, prefer_link) for c in node["content"])
    return ""

def extract_text_blocks(adf):
    """Extract text outside of tables."""
    text_blocks = []
    def traverse(content):
        for item in content:
            if item.get("type") == "table":
                continue
            if item.get("type") in ["paragraph", "listItem"]:
                text = "".join(extract_text_from_node(c) for c in item.get("content", []))
                if text.strip():
                    text_blocks.append(text.strip())
            if "content" in item and item.get("type") != "table":
                traverse(item["content"])
    traverse(adf.get("content", []))
    return "\n".join(text_blocks).strip() if text_blocks else ""

def extract_tables(adf):
    """Extract tables with link priority."""
    tables = []
    def traverse(content):
        for item in content:
            if item.get("type") == "table":
                headers = []
                rows_data = []
                for idx, row in enumerate(item.get("content", [])):
                    cells = []
                    for cell in row.get("content", []):
                        cell_text = ""
                        if "content" in cell:
                            cell_text = "".join(extract_text_from_node(c, prefer_link=True) for c in cell["content"])
                        cells.append(cell_text.strip())
                    if idx == 0:
                        headers = cells
                    else:
                        rows_data.append(cells)
                tables.append({"headers": headers, "rows": rows_data})
            elif "content" in item:
                traverse(item["content"])
    traverse(adf.get("content", []))
    return tables

def combine_text_and_tables(adf):
    """Convert ADF JSON to a structured DataFrame."""
    merged_text = extract_text_blocks(adf)
    tables = extract_tables(adf)
    dfs = []

    for t in tables:
        headers = t["headers"]
        df = pd.DataFrame(t["rows"], columns=headers)
        dfs.append(df)

    if merged_text:
        text_df = pd.DataFrame([{"Description": merged_text}])
        dfs.append(text_df)

    if dfs and len(dfs) > 1:
        final_df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
    elif dfs:
        final_df = dfs[0]
    else:
        final_df = pd.DataFrame(columns=["Description"])
    return final_df

# ------------------ MERGE ALL JIRA ISSUES ------------------
all_issue_dfs = []

for key in ISSUE_KEYS:
    issue = fetch_jira_issue(key)
    fields = issue.get("fields", {})
    
    description_adf = fields.get("description")  # This is ADF JSON
    if description_adf:
        df = combine_text_and_tables(description_adf)
        # Add issue key for reference
        df["IssueKey"] = key
        # Add other fields if needed
        for f in FIELDS:
            if f != "description":
                df[f] = str(fields.get(f, ""))
        all_issue_dfs.append(df)

# Merge all issues into one DataFrame
if all_issue_dfs:
    final_df = pd.concat(all_issue_dfs, axis=0, ignore_index=True, sort=True)
else:
    final_df = pd.DataFrame()

display(final_df)


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
            return urls[0]
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

# CELL ********************

import pandas as pd

var1 = "PCF-47824"
var2 = {
  "type": "doc",
  "version": 1,
  "content": [
    {
      "type": "orderedList",
      "attrs": {
        "order": 1
      },
      "content": [
        {
          "type": "listItem",
          "content": [
            {
              "type": "paragraph",
              "content": [
                {
                  "type": "text",
                  "text": "Merchant_Audience_Management_Service"
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "type": "paragraph",
      "content": [
        {
          "type": "text",
          "text": "      Branch: Production"
        },
        {
          "type": "hardBreak"
        },
        {
          "type": "text",
          "text": "     CommitID:  e768901ac1ae39276a7a70d79e761b0380e5f5ba "
        }
      ]
    },
    {
      "type": "paragraph",
      "content": [
        {
          "type": "text",
          "text": "     PR: "
        },
        {
          "type": "text",
          "text": "Audience new and existing logic by priyanshu-panwar_pinegit \u00b7 Pull Request #27 \u00b7 PineLabs-Merchant/Merchant_Audience_Management_Service",
          "marks": [
            {
              "type": "link",
              "attrs": {
                "href": "https://github.com/PineLabs-Merchant/Merchant_Audience_Management_Service/pull/27"
              }
            }
          ]
        }
      ]
    },
    {
      "type": "orderedList",
      "attrs": {
        "order": 2
      },
      "content": [
        {
          "type": "listItem",
          "content": [
            {
              "type": "paragraph",
              "content": [
                {
                  "type": "text",
                  "text": "Merchant_Campaign_Execution_Service"
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "type": "paragraph",
      "content": [
        {
          "type": "text",
          "text": "      Branch: Production"
        },
        {
          "type": "hardBreak"
        },
        {
          "type": "text",
          "text": "     CommitID:  43892d09ec3cb602169bcc4f4f6e461c4fc8ad4a"
        },
        {
          "type": "hardBreak"
        },
        {
          "type": "text",
          "text": "     PR: "
        },
        {
          "type": "text",
          "text": "Pp/cherry pick change for new and existing by avinash-rana_pinegit \u00b7 Pull Request #104 \u00b7 PineLabs-Merchant/Merchant_Campaign_Execution_Service",
          "marks": [
            {
              "type": "link",
              "attrs": {
                "href": "https://github.com/PineLabs-Merchant/Merchant_Campaign_Execution_Service/pull/104/files"
              }
            }
          ]
        }
      ]
    }
  ]
}

check = extract_text_from_description(var2)
print(check)
# Create DataFrame with a single row
# data = [(var1, var2)]

# jt_df = spark.createDataFrame(data, ["Ticket_ID", "Description"])

# display(jt_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
