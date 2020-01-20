import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.window import Window


def generate_window(order_col, rowrange=None, partitions=None):
    window = Window().orderBy(order_col)

    # for future reference: if we want to use partitions
    # if partitions is not None:
    #     window = window.partitionBy(partitions)

    if rowrange is not None:
        window = window.rowsBetween(*rowrange)

    return window


def over(column, value, window):
    return func.sum((column > value).astype('int')).over(window)


def under(column, value, window):
    return func.sum((column < value).astype('int')).over(window)


def get_control_range(colname, sigma=1):
    """
    Returns the lower and upper thresholds for a column (colname).
    Thresholds are the standard deviations n times (sigma) from the mean.
    """
    mean = col('{}_mean'.format(colname))
    std = col('{}_std'.format(colname))

    lower, upper = mean - sigma * std, mean + sigma * std

    return lower, upper


def compute_stats(df, colname):
    column = col(colname)
    meancol = func.mean(column).alias('{}_mean'.format(colname))
    stdcol = func.stddev(column).alias('{}_std'.format(colname))

    results = df.select(meancol, stdcol).first()
    mean = results['{}_mean'.format(colname)]
    std = results['{}_std'.format(colname)]

    return mean, std
