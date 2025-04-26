from Amz_Cloud.fetch_helper import build_noaa_url, fetch_and_store_from_api

url = build_noaa_url(
    datasetid="GHCND",
    startdate="2024-01-01",
    enddate="2024-01-02",
    stationid="GHCND:USW00094728",
    limit=1000
)

print("Generated NOAA API URL:", url)

# Fetch data from NOAA and upload to S3 bucket
s3_uri = fetch_and_store_from_api(source_url=url, dataset_name="noaa")

print("Dataset successfully stored at:", s3_uri)
