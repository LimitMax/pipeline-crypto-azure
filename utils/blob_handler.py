from azure.storage.blob import BlobServiceClient
import datetime, os, io, logging
from dotenv import load_dotenv

load_dotenv()

BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = "crypto-raw"

def connect_blob():
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)

def save_raw_to_blob(blob_client, symbol, df, folder="incremental", file_format="parquet"):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    container_client = blob_client.get_container_client(CONTAINER)

    if file_format == "parquet":
        blob_name = f"{folder}/{symbol}_{ts}.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        buffer.seek(0)
        container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
    else:  # fallback JSON
        blob_name = f"{folder}/{symbol}_{ts}.json"
        raw_json = df.to_json(orient="records", date_format="iso")
        container_client.upload_blob(name=blob_name, data=raw_json, overwrite=True)

    logging.info(f"☁️ Saved raw {symbol} to Blob: {blob_name}")
