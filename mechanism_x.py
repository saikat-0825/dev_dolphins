import pandas as pd
import boto3
import io
import time
from gdrive_auth import get_drive

# Settings
CHUNK_SIZE = 10000
S3_BUCKET_NAME = 'test-devdolphins'
S3_FOLDER = 'transactions-chunks/'
TRANSACTIONS_FILE_NAME = 'transactions.csv'

# Initialize S3 client
s3 = boto3.client('s3')

# Track state
current_start = 0
chunk_index = 0


def get_file_id(drive, filename):
    files = drive.ListFile({'q': f"title='{filename}' and trashed=false"}).GetList()
    if not files:
        raise FileNotFoundError(f"{filename} not found in Drive")
    return files[0]['id']


def download_chunk(drive, file_id, skip, chunk_size):
    file = drive.CreateFile({'id': file_id})
    content = file.GetContentString()
    df = pd.read_csv(io.StringIO(content), skiprows=list(range(1, skip + 1)), nrows=chunk_size)
    return df


def upload_chunk(df, index):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{S3_FOLDER}transactions_chunk_{index:06}.csv",
        Body=csv_buffer.getvalue()
    )


def main():
    global current_start, chunk_index
    drive = get_drive()
    file_id = get_file_id(drive, TRANSACTIONS_FILE_NAME)

    while True:
        try:
            df_chunk = download_chunk(drive, file_id, current_start, CHUNK_SIZE)
            if df_chunk.empty:
                print("All data processed.")
                break

            upload_chunk(df_chunk, chunk_index)
            print(f" Uploaded chunk {chunk_index} with {len(df_chunk)} rows.")

            current_start += len(df_chunk)
            chunk_index += 1
        except Exception as e:
            print(f" Error: {e}")

        time.sleep(1)  # Wait for 1 second


if __name__ == '__main__':
    main()
