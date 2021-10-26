import os
import json
import logging
import argparse
from subprocess import call
from datetime import datetime as dt
from pyspark.sql import SparkSession
from jobs.etl_tasks import run_job


def _parse_args():
    """ Parses the arguments from job submit request"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True)
    return parser.parse_args()


def _create_logger(log_filepath: str):
    """
    Returns a logger object for the entire execution plan
    """
    logger = logging.getLogger("main_logger")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: %(message)s")
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def main():
    # Process input file and output directory from job args
    inp_fname = _parse_args().filename
    inp_parts = inp_fname.split('_')
    out_dir = f"pq_{inp_parts[0]}_{'_'.join(inp_parts[2].split('.')[0].split('-'))}"

    root_dir = os.path.abspath('')
    with open(os.path.join(root_dir, "configs", "app_config.json"), 'r') as f:
        app_config = json.load(f)

    with open(os.path.join(root_dir, "configs", "cols_config.json"), 'r') as f:
        cols_config = json.load(f)

    # Configure and create log file
    date_part = dt.strftime(dt.now(), "%Y%m%d_%H_%M")
    log_fname = f"logs_{out_dir[3:]}_{date_part}.log"
    log_fpath = os.path.join(root_dir, 'logs', log_fname)
    logger = _create_logger(log_fpath)

    spark = SparkSession.builder.appName(
        app_config.get("app_name")).getOrCreate()
    logger.info("Spark session has been created")

    # Run ETL job
    run_job(spark, inp_fname, out_dir, app_config, cols_config)
    logger.info("Finished running jobs. Stopping spark session...")

    # Call shell subprocess to copy log file to Storage
    # call(["gsutil", "cp", log_fpath, app_config.get("logs_dir")])

    # Stop spark session
    spark.stop()


if __name__ == "__main__":
    main()
