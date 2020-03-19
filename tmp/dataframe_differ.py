"""
1. Check if dataframes with same set of rows but differently sorted will return the expected (that is equal)
    Answer: Yes, they are
2. Read features:
    read from csv's
    read from parquet's
    read from folders (both csv, parquet)
    read from S3

3. Output features:
    add difference in data type
    write as json
    write as excel spreadsheet
"""

import argparse
import glob
import json
import logging
import os
from functools import reduce
from pyspark.sql import SparkSession

def main():

    # LOGGER = logging.getLogger('pyspark')
    # LOGGER.info('Starting program...')

    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", help="")
    args = parser.parse_args()

    if args.config_file is not None:
        with open(args.config_file) as json_config:
            try:
                config = json.load(json_config)
            except json.JSONDecodeError:
                raise ValueError("Wrong JSON format!")

            if config['sparkJobName'] is None:
                raise ValueError("Specify Spark job name!")

            is_source_valid, message = validate_config_data_source(config['source1'])
            if not is_source_valid:
                raise ValueError('Error in config source1: %s'.format(message))

            is_source_valid, message = validate_config_data_source(config['source2'])
            if not is_source_valid:
                raise ValueError('Error in config source2: %s'.format(message))

            if config['primaryKey'] is None:
                raise ValueError("Specify column name of the primary key!")

            if config['output'] is None:
                raise ValueError("Specify name of output file (of type json) for this job!")

        print("Creating Spark Session...")

        spark = SparkSession.builder.appName(config['sparkJobName']).master('local[*]').enableHiveSupport().getOrCreate()

        print("Spark Session created")
        # sc = spark.sparkContext
        # log4jLogger = sc._jvm.org.apache.log4j
        # LOGGER = log4jLogger.LogManager.getLogger(__name__)
        # LOGGER.info("Spark Session created")

        df1 = read(spark, config['source1'])
        df2 = read(spark, config['source2'])

        # check if there are columns to be compared from each side are specified
        # the two should be corresponding in sequence
        if "columns_to_compare" in config['source1'] or "columns_to_compare" in config['source2']:
            cols_df1 = config['source1']['columns_to_compare'] if "columns_to_compare" in config['source1'] else df1.schema.names
            cols_df2 = config['source2']['columns_to_compare'] if "columns_to_compare" in config['source2'] else df2.schema.names

            df1, df2 = apply_columns_to_compare(df1, cols_df1, df2, cols_df2)

        # LOGGER.info("The data sources are loaded to the dataframes. Calculating differences.")
        print("The data sources are loaded to the dataframes. Has {} and {} row count respectively".format(df1.count(), df2.count()))

        has_difference, result = get_diff(df1, df2, config['primaryKey'])

        if not has_difference:
            print(result['message'])
        else:
            print(result['message'])
            if "difference" in result:
                print('Writing results to file {}'.format(config['output']))
                with open(config['output'], 'w') as f:
                    json.dump(result['difference'], f, ensure_ascii=False, indent=2)

        print("Stopping Spark Session")
        spark.stop()

    else:
        raise ValueError("Please specify the configuration file!")

# Validate option
def validate_config_data_source(source):

    if source is None:
        return False, "Specify data source type: csv, parquet, or hive!"
    else:
        if source['type'] == 'csv':
            if source['hasHeader'] is None or source['separator'] is None or source['filepath'] is None:
                return False, "Missing hasHeader, separator, and/or filepath arguments in config file for type csv data source!"
        elif source['type'] == 'hive':
            if source['query'] is None:
                return False, "Missing query string in config file for type hive data source!"
        elif source['type'] == 'parquet':
            if ['source']['filepath'] is None:
                return False, "Missing filepath argument in config file for type parquet data source!"
        else:
            return False, "Data source type should be: csv, parquet, or hive!"

    return True, None

# Generic wrapper function to read source data and return a dataframe
def read(spark, source):

    if source['type'] == 'hive':
        return spark.sql(source['query'])
    elif source['filepath'].startswith('s3'):
        return read_from_s3(spark, source['filepath'], source['type'], source['hasHeader'], source['separator'])
    else:
        return read_from_local(spark, source['filepath'], source['type'], source['hasHeader'], source['separator'])

# Get dataframe from AWS S3
def read_from_s3(spark, filepath, fileformat, header=None, separator=None):

    if (fileformat == 'csv'):
        df = spark.read.csv(filepath, header=header, separator=separator)
    elif (fileformat == 'parquet'):
        df = spark.read.csv(filepath)
    else:
        raise ValuError("File format must be either parquet or csv!")

    return df

# Get dataframe from local file system
def read_from_local(spark, filepath, fileformat, header=None, separator=None):
    # We assume this is a local file system
    if filepath.endswith('/'):
        filepath = filepath[:-1]

    if (os.path.isdir(filepath) and glob.glob('filepath/*.csv')) or filepath[-4:] == '.csv':
        df = spark.read.csv(filepath, header=header, sep=separator)
    elif (os.path.isdir(filepath) and glob.glob('filepath/*.parquet')) or filepath[-8:] == '.parquet':
        df = spark.read.parquet(filepath)
    else:
        raise ValueError("No valid csv or parquet file/files in filepath!")

    return df

# Get difference between two dataframes who have rows with same keys
def get_diff_same_keys(df1, df2, column_names, pk, same_keys):
    diff_cols = []

    for id in same_keys:
        row1 = df1.filter(df1[pk] == id).first()
        row2 = df2.filter(df2[pk] == id).first()
        cols = []

        for col in column_names:
            if row1[col] != row2[col]:
                cols.append( {col: { "a": row1[col], "b": row2[col] } } )

        diff_cols.append( {pk: id, "columns": cols} )

    return diff_cols


def apply_columns_to_compare(a, cols_a, b, cols_b):

    a = a.select(cols_a.split(','))
    old_schema_a = a.schema.names
    new_schema_a = [col[col.rfind(".")+1:].lower() for col in old_schema_a]
    a = reduce(lambda a, idx: a.withColumnRenamed(old_schema_a[idx], new_schema_a[idx]), xrange(len(old_schema_a)), a)

    b = b.select(cols_b.split(','))
    old_schema_b = b.schema.names
    new_schema_b = [col[col.rfind(".")+1:].lower() for col in schema_for_b.split(',')]
    b = reduce(lambda b, idx: b.withColumnRenamed(old_schema_b[idx], new_schema_b[idx]), xrange(len(old_schema_b)), b)

    return a, b

# Get difference between two dataframes
def get_diff(a, b, pk):

    # 1. Check first if schemas are the same
    # Todo: Add schema differences in the dictionary
    if a.schema.names != b.schema.names:
        return True, { "message" : "Schemas do not match." }


    # 2. Check if same number of rows
    # May not be useful after all, as we are inspecting per row

    # 3. (Symmetric difference) Eliminate intersection of rows from each side: if nothing remains on both side, it means they are equal
    a_minus_b = a.subtract(a.intersect(b))
    b_minus_a = b.subtract(b.intersect(a))

    if len(a_minus_b.take(1)) == 0 and len(b_minus_a.take(1)) == 0:
        return False, { "message": "Both are equal." }

    # 4. From the symmetric difference, find the rows that has the same keys on both side: it means that some column values for these same-key rows do not match
    result_diff = { "a_not_in_b": [], "b_not_in_a": [], "same_key_but_diff_values": [] }
    result_diff['same_key_but_diff_values'] = sorted(result_diff['same_key_but_diff_values'], key = lambda x: x[pk])

    column_names = a.schema.names

    a_pks = a_minus_b.select(pk).rdd.flatMap(lambda x: x).collect()
    b_pks = b_minus_a.select(pk).rdd.flatMap(lambda x: x).collect()
    same_key_diff_val = list(set(a_pks) & set(b_pks))

    result_diff['same_key_but_diff_values'] = get_diff_same_keys(a_minus_b, b_minus_a, column_names, pk, same_key_diff_val)
    result_diff['same_key_but_diff_values'] = sorted(result_diff['same_key_but_diff_values'], key = lambda x: x[pk])

    # 5. List the symmetric difference with the exception from step 4
    if len(a_minus_b.take(1)) > 0:
        a_minus_b_2 = a_minus_b.subtract(a_minus_b[a_minus_b[pk].isin(same_key_diff_val)])
        result_diff['a_not_in_b'] = list(a_minus_b_2.select(pk).toPandas()[pk])
        result_diff['a_not_in_b'].sort()

    if len(b_minus_a.take(1)) > 0:
        b_minus_a_2 = b_minus_a.subtract(b_minus_a[b_minus_a[pk].isin(same_key_diff_val)])
        result_diff['b_not_in_a'] = list(b_minus_a_2.select(pk).toPandas()[pk])
        result_diff['b_not_in_a'].sort()

    return True, { "message": "Mismatch on some rows.", "difference": result_diff }

if __name__ == "__main__":
    main()
