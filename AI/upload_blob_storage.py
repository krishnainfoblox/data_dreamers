from azure.storage.blob import BlobServiceClient

blob_service_client = BlobServiceClient.from_connection_string("your_connection_string")
container_client = blob_service_client.get_container_client("your_container_name")
blob_client = container_client.get_blob_client("data.csv")

with open("data.csv", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
