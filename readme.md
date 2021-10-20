# Processing NYC Taxi Dat using PySpark ETL pipeline
## Description
This is an project to extract, transform, and load large amount of data from [NYC Taxi Rides](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) database (Hosted on AWS S3). It extracts data from CSV files of large size (~2GB per month) and applies transformations such as datatype conversions, drop unuseful rows/columns, etc. Finally, the data is written back in parquet format. This saves time for tasks such as machine learning. It also saves a huge amount of space (~97% space reduction from csv to parquet) making it easy to store for downstream tasks.

## How to use it (Using GCP as the cloud service of choice)
- Setup a bucket on Google Cloud Storage
- Use get_raw_data.sh to download raw data from s3 in the form of CSV files to the GCS bucket
- Setup a GCP dataproc service
- SSH into the master node and copy the entire project folder to the Persistent Disk
- Edit the [configuration file](https://github.com/unni-krrish/pyspark-etl-nyc-taxi/blob/main/configs/app_config.json) for application
- Submit the job: `submit-spark main.py --filename [raw_data_filename]` or Execute [submit_job.sh](https://github.com/unni-krrish/pyspark-etl-nyc-taxi/blob/main/submit_job.sh) with appropriate args

## Project structure
```bash
root/
|---bash/
    |---create_cluster.sh
    |---install.sh
|---configs/
    |---app_config.json
    |---cols_config.json
|---jobs/
    |---etl_tasks.py
    |---transformations.py
|   get_raw_data.sh
|   main.py
|   requirements.txt
|   submit_job.sh
```