# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from datetime import datetime, timedelta

start_date = (datetime.utcnow() - timedelta(days=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
print(start_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

""" 
InvoiceItem i 
LEFT JOIN Account a ON i.AccountId = a.Id 
LEFT JOIN Invoice iv ON i.InvoiceId = iv.Id 
LEFT JOIN Product p ON i.ProductId = p.Id 
LEFT JOIN RatePlan r ON i.RatePlanId = r.Id 
LEFT JOIN Subscription s ON i.SubscriptionId = s.Id 
""" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

daily_adds_schema = StructType([
    StructField("RatePlanId", StringType(), True),
    StructField("RatePlanName", StringType(), True),
    StructField("ProductRatePlanId", StringType(), True),
    StructField("RatePlanAmendmentType", StringType(), True),
    StructField("RateplanCreatedDate", StringType(), True),
    StructField("RatePlanUpdatedDate", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("ProductFamily", StringType(), True),
    StructField("AccountId", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("AccountName", StringType(), True),
    StructField("UserId", StringType(), True),
    StructField("AccountCRMId", StringType(), True),
    StructField("AccountStatus", StringType(), True),
    StructField("APM", StringType(), True),
    StructField("AccountCreatedDate", StringType(), True),
    StructField("AccountUpdatedDate", StringType(), True),
    StructField("SubscriptionId", StringType(), True),
    StructField("SubscriptionName", StringType(), True),
    StructField("SubscriptionOriginalId", StringType(), True),
    StructField("SubscriptionRenewalTerm", StringType(), True),
    StructField("SubscriptionCurrency", StringType(), True),
    StructField("PreviousSubscriptionId", StringType(), True),
    StructField("SubscriptionStatus", StringType(), True),
    StructField("SubscriptionTermType", StringType(), True),
    StructField("SubscriptionVersion", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("SubscriptiontermStartDate", StringType(), True),
    StructField("SubscriptiontermEndDate", StringType(), True),
    StructField("SubscriptionCancelledDate", StringType(), True),
    StructField("SubscriptionCreatedDate", StringType(), True),
    StructField("SubscriptionUpdatedDate", StringType(), True),
    StructField("OriginalCreatedDate", StringType(), True),
    StructField("InvoiceNumber", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("InvoiceItemCreatedDate", StringType(), True),
    StructField("MRR", DoubleType(), True),
    StructField("TCV", DoubleType(), True),
    StructField("TaxAmount", DoubleType(), True),
    StructField("TotalAddsRevenue", DoubleType(), True)
])
 
# -------------------- 4 days back for initial fetch --------------------
start_date = (datetime.utcnow() - timedelta(days=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
 
daily_adds_query = f"""
    SELECT 
        R.Id as RatePlanId, 
        R.Name as RatePlanName,
       PRP.ratePlanId__c as ProductRatePlanId,
       R.AmendmentType as RatePlanAmendmentType,
       R.CreatedDate as RateplanCreatedDate,
       R.UpdatedDate as RatePlanUpdatedDate,
       P.Name as ProductName,
       P.ProductFamily__c as ProductFamily,
       A.Id as AccountId,
       A.AccountNumber as AccountNumber,
       A.Name AS AccountName,
       A.userId__c AS UserId,
       A.CRMId as AccountCRMId,
       A.Status as AccountStatus,
       A.APM__c as APM,
       A.CreatedDate as AccountCreatedDate,
       A.UpdatedDate as AccountUpdatedDate,
       S.Id as SubscriptionId,
       S.Name as SubscriptionName,
       S.OriginalId as SubscriptionOriginalId,
       S.RenewalTerm as SubscriptionRenewalTerm,
       S.Currency as SubscriptionCurrency,
       S.PreviousSubscriptionId as PreviousSubscriptionId,
       S.Status as SubscriptionStatus,
       S.TermType as SubscriptionTermType,
       S.Version as SubscriptionVersion,
       S.displayName__c as DisplayName,
       S.termStartDate as SubscriptiontermStartDate,
       S.termEndDate as SubscriptiontermEndDate,
       S.CancelledDate as SubscriptionCancelledDate,
       S.CreatedDate as SubscriptionCreatedDate,
       S.UpdatedDate as SubscriptionUpdatedDate,
       S.OriginalCreatedDate AS OriginalCreatedDate,
       IV.InvoiceNumber as InvoiceNumber,
       IV.InvoiceDate as InvoiceDate,
       I.CreatedDate as InvoiceItemCreatedDate,
       SUM(RPC.MRR) as MRR,
       SUM(RPC.TCV) as TCV,
       I.TaxAmount as TaxAmount,
       SUM(I.ChargeAmount) as TotalAddsRevenue
FROM InvoiceItem I
LEFT JOIN Account A ON I.AccountId = A.Id 
LEFT JOIN Invoice IV ON I.InvoiceId = IV.Id 
LEFT JOIN Product P ON I.ProductId = P.Id 
LEFT JOIN RatePlan R ON I.RatePlanId = R.Id 
LEFT JOIN Subscription S ON I.SubscriptionId = S.Id 
LEFT JOIN RatePlanCharge RPC ON R.Id = RPC.RatePlanID
LEFT JOIN ProductRatePlan PRP ON P.ProductId = PRP.Id
WHERE 
    R.CreatedDate >= TIMESTAMP '{start_date}'
  AND S.Currency='USD'
  AND S.Version = 1
  AND ((NOT S.displayName__c LIKE 'yqa%' AND NOT S.displayName__c LIKE 'ysbsqa%')
       AND (NOT Account.userId__c LIKE 'yqa%'))
  AND DATE(I.CreatedDate)=DATE(R.CreatedDate)
GROUP BY R.Id, R.Name, R.AmendmentType, R.CreatedDate, R.UpdatedDate,
         P.Name, P.ProductFamily__c, A.Id, A.AccountNumber, A.CRMId, A.Status,
         PRP.ratePlanId__c, A.APM__c, A.Name, A.userId__c,
         A.CreatedDate, A.UpdatedDate,
         S.Id, S.Name, S.OriginalId,
         S.RenewalTerm, S.Currency, S.PreviousSubscriptionId, S.OriginalCreatedDate,
         S.Status, S.TermType, S.Version,
         S.termStartDate, S.termEndDate, S.CancelledDate, S.displayName__c,
         I.TaxAmount, IV.InvoiceDate, IV.InvoiceNumber,
         S.CreatedDate, S.UpdatedDate, I.CreatedDate
ORDER BY RatePlan.CreatedDate ASC
LIMIT 100000
"""
print("✔ Cell 2 initialized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DEBUG = True
 
def debug_log(message):
    if DEBUG:
        print(f"🪶 {message}")
 
print("🔐 Authenticating with Zuora...")
auth_url = f"{ZUORA_BASE_URL}/oauth/token"
debug_log(f"Auth URL: {auth_url}")
 
auth_resp = requests.post(
    auth_url,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
)
 
debug_log(f"Auth Response Code: {auth_resp.status_code}")
 
if auth_resp.status_code != 200:
    raise Exception(f"❌ Authentication failed: {auth_resp.text}")
 
ACCESS_TOKEN = auth_resp.json()["access_token"]
debug_log(f"Access Token: {ACCESS_TOKEN}")
print("✅ Authentication successful!")
 
 
def fetch_sample_records_data_query(zql_query):
    """Use the newer Data Query API (async, better for large queries)"""
    job_url = f"{ZUORA_BASE_URL}/query/jobs"
    zql_query_cleaned = " ".join(zql_query.split())
    debug_log(f"Using Data Query API: {job_url}")
    debug_log(f"Query:\n{zql_query_cleaned}")
 
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
 
    # Try with json parameter
    payload = {
  "query": zql_query_cleaned,
  "compression": "NONE",
 
  "output": {
    "target": "S3"
  },
  "outputFormat": "JSON"
}
    debug_log(f"Sending payload: {json.dumps(payload)}")
    resp = requests.post(job_url, headers=headers, json=payload)
    print(resp)
    debug_log(f"Submit Response: {resp.status_code} - {resp.text}")
 
    if resp.status_code != 200:
        raise Exception(f"❌ Query submission failed: {resp.text}")
 
    job_id = resp.json()["data"]["id"]
    print(f"🆔 Job ID: {job_id}")
 
    # Poll for completion
    while True:
        status_url = f"{job_url}/{job_id}"
        status_resp = requests.get(status_url, headers=headers)
        status = status_resp.json()["data"]["queryStatus"]
        debug_log(f"Polling {status_url} → {status}")
 
        status_data = status_resp.json()
        status = status_resp.json()["data"]["queryStatus"]
        debug_log(f"Job status: {status}")
 
        if status in ["completed", "failed", "aborted", "cancelled"]:
            break
        time.sleep(10)
 
    if status in ["failed", "aborted", "cancelled"]:
        raise Exception(f"❌ Query job failed with status: {status}")
 
    # Get results file URL
    # data_file = status_data.get("dataFile")
    data_file_link = status_data["data"]["dataFile"]
    record_count = status_data["data"].get("outputRows", "Unknown")
    debug_log(f"Record_Count: {record_count}")
 
    if not data_file_link:
        raise Exception(f"❌ No data file in response: {status_data}")
    debug_log(f"Downloading results from: {data_file_link}")
    # Download the results
    results_resp = requests.get(data_file_link)
    debug_log(f"Download Response Code: {results_resp.status_code}")
 
    if results_resp.status_code != 200:
        raise Exception(f"❌ Failed to download results: {results_resp.text}")
 
    # Parse JSON lines format
    data = []
    for line in results_resp.text.strip().split('\n'):
        if line:
            data.append(json.loads(line))
    debug_log(f"Fetched {len(data)} records")
 
    if DEBUG and len(data) > 0:
        debug_log("Sample record:\n" + json.dumps(data[0], indent=2))
 
    return data
 
 
try:
    invoice_item_records = fetch_sample_records_data_query(daily_adds_query)
   
    print(f"📊 InvoiceItem → {len(invoice_item_records)} records fetched")
   
   
    pdf_invoice_item = pd.json_normalize(invoice_item_records, sep="_")
   
 
    df_invoice_item = spark.createDataFrame(pdf_invoice_item, schema = daily_adds_schema)
   
    df_invoice_item.write.mode("overwrite").format("delta").saveAsTable("Zuora_DailyAdds")
   
    print("💾 Debug sample data written successfully to Fabric Lakehouse ✅")
 
except Exception as e:
    print(f"\n❌ Error occurred: {str(e)}")
    print("\n💡 If Data Query API fails, try setting USE_ACTION_API = True at the top of the script")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
