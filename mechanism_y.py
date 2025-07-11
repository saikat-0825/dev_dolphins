import boto3
import pandas as pd
import psycopg2
import json
import io
import time
from datetime import datetime
import pytz

# AWS Config
bucket_name = 'test-devdolphins'
chunk_prefix = 'transactions/'
detection_prefix = 'detections/'
importance_path = 'config/CustomerImportance.csv'

# PostgreSQL Config
pg_conn = psycopg2.connect(
    host="localhost",
    database="mechanism_y_db",
    user="postgres",
    password="admin",
    
    port = '5432'
)

    


pg_cursor = pg_conn.cursor()

# S3 client
s3 = boto3.client('s3')

# Timezone
IST = pytz.timezone('Asia/Kolkata')
YStartTime = datetime.now(IST).isoformat()

# Load Customer Importance from S3
obj = s3.get_object(Bucket=bucket_name, Key=importance_path)
importance_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
importance_df.columns = importance_df.columns.str.strip()

# Ensure temp table exists
pg_cursor.execute("""
CREATE TABLE IF NOT EXISTS merchant_customer_summary (
    customername TEXT,
    merchantid TEXT,
    transaction_count INT,
    total_value FLOAT,
    gender TEXT,
    PRIMARY KEY (customername, merchantid)
);
""")
pg_conn.commit()

# List all chunk files
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=chunk_prefix)
chunk_keys = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.csv')]

detections = []

for key in chunk_keys:
    print(f"[INFO] Processing chunk: {key}")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df.columns = df.columns.str.strip()

    # Normalize column names
    df.columns = df.columns.str.lower()

    # Update summary table in Postgres
    for _, row in df.iterrows():
        pg_cursor.execute("""
            INSERT INTO merchant_customer_summary (customername, merchantid, transaction_count, total_value, gender)
            VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (customername, merchantid)
            DO UPDATE SET
                transaction_count = merchant_customer_summary.transaction_count + 1,
                total_value = merchant_customer_summary.total_value + EXCLUDED.total_value;
        """, (row['customername'], row['merchantid'], row['transactionvalue'], row.get('gender', '')))
    pg_conn.commit()

    # Fetch summary for pattern checks
    pg_cursor.execute("SELECT * FROM merchant_customer_summary;")
    summary = pg_cursor.fetchall()
    summary_df = pd.DataFrame(summary, columns=['customerName', 'merchantId', 'transactionCount', 'totalValue', 'gender'])

    # Pattern 1
    merchant_groups = summary_df.groupby('merchantId')
    for merchant_id, group in merchant_groups:
        total_txns = group['transactionCount'].sum()
        if total_txns < 50000:
            continue
        top10_cutoff = group['transactionCount'].quantile(0.90)
        bottom10_weighted_cutoff = importance_df['weight'].quantile(0.10)

        for _, row in group.iterrows():
            imp_weight = importance_df[
                (importance_df['customerName'] == row['customerName']) &
                (importance_df['merchantId'] == merchant_id)
            ]['weight'].mean()

            if row['transactionCount'] >= top10_cutoff and imp_weight <= bottom10_weighted_cutoff:
                detections.append({
                    "YStartTime": YStartTime,
                    "detectionTime": datetime.now(IST).isoformat(),
                    "patternId": "PatId1",
                    "ActionType": "UPGRADE",
                    "customerName": row['customerName'],
                    "MerchantId": merchant_id
                })

    # Pattern 2
    for _, row in summary_df.iterrows():
        avg_value = row['totalValue'] / row['transactionCount']
        if avg_value < 23 and row['transactionCount'] >= 80:
            detections.append({
                "YStartTime": YStartTime,
                "detectionTime": datetime.now(IST).isoformat(),
                "patternId": "PatId2",
                "ActionType": "CHILD",
                "customerName": row['customerName'],
                "MerchantId": row['merchantId']
            })

    # Pattern 3
    merchant_gender = summary_df.groupby(['merchantId', 'gender']).size().unstack(fill_value=0).reset_index()
    for _, row in merchant_gender.iterrows():
        female = row.get('Female', 0)
        male = row.get('Male', 0)
        if female > 100 and female < male:
            detections.append({
                "YStartTime": YStartTime,
                "detectionTime": datetime.now(IST).isoformat(),
                "patternId": "PatId3",
                "ActionType": "DEI-NEEDED",
                "customerName": "",
                "MerchantId": row['merchantId']
            })

    # Write detections in batches of 50
    while len(detections) >= 50:
        batch = detections[:50]
        detections = detections[50:]
        filename = f"{detection_prefix}detections_{datetime.now(IST).strftime('%Y-%m-%dT%H-%M-%S')}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(batch)
        )
        print(f"[INFO] Wrote detection batch to {filename}")

# Write remaining detections
if detections:
    filename = f"{detection_prefix}detections_{datetime.now(IST).strftime('%Y-%m-%dT%H-%M-%S')}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=json.dumps(detections)
    )
    print(f"[INFO] Wrote final detection batch to {filename}")

pg_cursor.close()
pg_conn.close()
