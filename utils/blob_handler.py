import datetime, io
from azure.storage.blob import BlobServiceClient
from utils import config
from utils.logger import logger
from utils.retry import with_retry


def connect_blob():
    """Connect ke Azure Blob Storage"""
    if not config.AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING tidak ditemukan di environment")
    return BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)


def _upload_blob(container_client, blob_name, data):
    """Helper upload blob sekali (dipanggil oleh with_retry)"""
    container_client.upload_blob(name=blob_name, data=data, overwrite=True)


def save_raw_to_blob(blob_client, symbol, df, folder="incremental", file_format="parquet"):
    """Simpan DataFrame ke Azure Blob Storage (Parquet atau JSON) dengan retry"""
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{folder}/{symbol}_{ts}.{file_format}"
    container_client = blob_client.get_container_client(config.BLOB_CONTAINER)

    if file_format == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        buffer.seek(0)
        data = buffer
    else:  # fallback ke JSON
        data = df.to_json(orient="records", date_format="iso")

    # Upload dengan retry
    with_retry(lambda: _upload_blob(container_client, blob_name, data), max_attempts=3, delay=2)

    logger.info(f"Saved raw {symbol} to Blob: {blob_name}")
    return blob_name
