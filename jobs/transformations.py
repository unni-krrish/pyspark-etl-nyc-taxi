import logging
import pyspark.sql.functions as F

logger = logging.getLogger("logger")


def check_columns(df, cols_config):
    """
    Performs a check on column names and their
    expected datatypes. 

    :param df : Input dataframe
    :param cols_config : List of column names loaded from config file
    """
    cleared = True
    curr_types = {x[0]: x[1] for x in df.dtypes}
    req_cols = cols_config.get('req_cols')
    req_types = cols_config.get('req_col_types')
    for col in req_cols:
        if col not in df.columns:
            logger.warning(f"Missing column in the input data: {col}")
            cleared = False
            continue
        if curr_types[col] != req_types[col]:
            logger.warning(
                f"Expected {req_types[col]} type for {col}. Got {curr_types[col]}")
            cleared = False
    return cleared


def convert_dates(df):
    """
    Returns input dataframe with pickup dates converted from
    string type to timestamp type.

    :param df : input dataframe
    """
    return (df.withColumn('Trip_Pickup_DateTime',
                          F.to_timestamp('Trip_Pickup_DateTime'))
            .withColumn('Trip_Dropoff_DateTime',
                        F.to_timestamp('Trip_Dropoff_DateTime')))


def get_duration(df):
    """
    Calculates trip duration using piuckup and
    dropoff times.

    :param df : input dataframe
    """
    return (df.withColumn('duration',
                          F.col('Trip_Dropoff_DateTime').cast('long')
                          - F.col('Trip_Pickup_DateTime').cast('long')))


def remove_negatives(df):
    num_cols = [x[0] for x in df.dtypes if x[1] in ['int', 'double']]
    neg_vals = {}
    filtered_df = df
    for col in num_cols:
        n = df.filter(F.col(col) < 0).count()
        if n > 0:
            neg_vals[col] = n
            filtered_df = filtered_df.filter(F.col(col) >= 0)
    print(f"Negative values found in :\n{neg_vals}")
    print(f"Removed rows with negative values")
    return filtered_df


def drop_columns(df):
    """
    Drop columns that are not useful or mostly
    contains too many null values.

    :param df : input dataframe
    """
    cols_to_drop = ('store_and_forward', 'Rate_Code', 'mta_tax')
    return df.drop(*cols_to_drop)
