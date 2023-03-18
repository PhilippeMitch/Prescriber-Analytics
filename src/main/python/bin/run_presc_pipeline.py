### Import all the necessary Modules
import os
import sys
import logging
import logging.config
from subprocess import Popen, PIPE

from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql import DataFrame

import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_current_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocess import perform_data_cleaning
from presc_run_data_transform import city_report, top_5_prescribers
from presc_run_data_extraction import extract_files
from presc_run_data_persist import data_persist_hive, data_persist_postgre

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        logging.info("main() is started !!! \n\n")
        ### Get Spark Object
        spark = get_spark_object(gav.envn, gav.appName)
        ### Validate Spark Object
        get_current_date(spark)

        ### Initiate run_pres_data_ingest Script
        ### Load the City File
        file_dir = gav.statging_dim_city
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema

        ### Validate run_data script for city Dimension dataframe
        df_city = load_files(spark, file_dir, file_format, header, inferSchema)

        ### Load the Prescriber Fact File
        file_dir = gav.statging_fact
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema

        df_fact = load_files(spark, file_dir, file_format, header, inferSchema)

        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        df_count(df_city, 'df_city')
        df_top10_rec(df_fact, 'df_fact')

        ### Initiate presc_run_data_process script
        ## Perform data Cleaning Operations for df_city
        df_city_sel, df_fact_sel = perform_data_cleaning(df_city, df_fact)

        ### Validation for df_city and df_fact
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        ### Initiate presc_run_data_transform script
        # develop City Report and Prescriber Report
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_prescribers(df_fact_sel)

        # Validation for df_city_final
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')

        # Validation for df_presc_final
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')

        ### Initiate the run_data_extraction_script
        CITY_PATH = gav.output_city
        extract_files(df_presc_final, 'json', CITY_PATH, 1, False, 'bzip2')

        PRESC_PATH = gav.output_fact
        extract_files(df_presc_final, 'orc', PRESC_PATH, 2, False, 'snappy')

        ### Persist Data
        # Persist data at Hive
        data_persist_hive(spark = spark, df = df_presc_final, dfName = 'df_presc_final', partitionBy = 'delivery_date', mode = 'append')
        data_persist_hive(spark = spark, df = df_city_final, dfName = 'df_city_final', partitionBy = 'delivery_date', mode = 'overwrite')
        #data_persist_hive(spark = spark, df = df_presc_final, dfName = 'df_presc_final', partitionBy = 'delivery_date', mode = 'overwrite')

        # Persist data into Postgre
        data_persist_postgre(
            spark = spark,
            df = df_city_final,
            dfName = 'df_city_final',
            url = 'jdbc:postgresql://localhost:6432/prescriberanalytics',
            driver = 'org.postgresql.Driver',
            dbtable = 'df_city_final',
            mode = 'append',
            user = os.environ.get('USER'),
            password = os.environ.get('PASSWORD')
        )

        data_persist_postgre(
            spark = spark,
            df = df_presc_final,
            dfName = 'df_presc_final',
            url = 'jdbc:postgresql://localhost:6432/prescriberanalytics',
            driver = 'org.postgresql.Driver',
            dbtable = 'df_presc_final',
            mode = 'append',
            user = os.environ.get('USER'),
            password = os.environ.get('PASSWORD')
        )

        ### End of Application part 1
        logging.info("presc_run_pipeline.py is Completed !!! \n\n")
    except Exception as exp:
        logging.error(
            "Error occurred in the main method. Please check the stack Trace to go to the respective module" + str(exp),
            exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info('run_presc_pipeline is Started !!! \n\n')
    main()
