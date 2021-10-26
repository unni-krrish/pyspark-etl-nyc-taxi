from time import perf_counter
import logging
from jobs.transformations import *

logger = logging.getLogger("main_logger")


def _extract_data(spark, input_fname, app_config):
    """
    Read the input data in the form of csv file.

    :param spark : Active spark session
    :param input_fname : Input filename as in the storage directory
    :param app_config : loaded json object with app configuration
    :returns : spark dataframe of input data
    """
    f_path = app_config.get("input_dir") + input_fname
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f_path)
    )


def _transform_data(raw_df, cols_config):
    """
    Applies required transformations to the raw dataframe

    :returns : Trasformed dataframe ready to be exported/loaded
    """
    # Perform column and dtype checks
    if check_columns(raw_df, cols_config):
        df = raw_df
    else:
        logger.warning("Inconsistencies found during column check")
    # Apply transformations
    df = convert_dates(df)
    df = get_duration(df)
    df = remove_negatives(df)
    df = drop_columns(df)
    return df


def _load_data(transformed_df, output_subdir, app_config):
    """
    Save data to parquet file
    """
    f_path = app_config.get("output_dir") + output_subdir
    transformed_df.write.mode("overwrite").parquet(f_path)


def run_job(spark, input_fname, output_subdir, app_config, cols_config):
    """
    Chains extract, transform and load tasks to execute a full ETL cycle.
    """
    logger.info("Started running jobs...")
    pt_1 = perf_counter()
    raw_df = _extract_data(spark, input_fname, app_config)
    raw_df.cache()
    pt_2 = perf_counter()
    logger.info(f"Took {pt_2-pt_1} for extraction, caching")
    logger.info("Applying transformations...")
    transformed_df = _transform_data(raw_df, cols_config)
    pt_3 = perf_counter()
    logger.info(f"Took {pt_3-pt_2} to transform data")
    logger.info("Loading transformed data into Parquet...")
    _load_data(transformed_df, output_subdir, app_config)
    pt_4 = perf_counter()
    logger.info(f"Took {pt_4-pt_3} to load data to parquet")
    logger.info(
        f"Total time taken for all ETL tasks : {(pt_4-pt_1)/60} minutes..!")
