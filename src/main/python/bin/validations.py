import logging
import logging.config

### Load the Logging Configuration File
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)


def get_current_date(spark):
    try:
        opDF = spark.sql(""" select current_date""")
        logger.info("Validate the Spark object by printing the current date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the methode - get_spark_current_date(). Please check the Stack Trace. " + str(exp))
        raise
    except Exception as exp:
        logger.error("Error in the methode - get_spark_current_date(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("Spark object is validated. Spark object is ready")


def df_count(df, dfName):
    try:
        logger.info(f"The DataFrame Validation by count df_count is started for Dataframe {dfName} !!! \n\n")
        df_count = df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info(f"The DataFrame Validation by count df_count() is completed !!! \n\n")


def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataFrame Validation by top record df_top10_rec() is started for DataFrame {dfName} !!! \n\n")
        logger.info(f"The DataFrame top 10 records are: ")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Validation by top 10 record df_top10_rec() is completed !!! \n\n")


def df_print_schema(df, dfName):
    try:
        logger.info(f"The DataFrame Schema Validation for DataFrame {dfName} !!! \n\n")
        sch = df.schema.fields
        logger.info(f'The DataFrame {dfName} schema is: ')
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_show_schema(). Please check the stack Trace. " + str(exp))
    else:
        logger.info("The dataFrame schema Validation is completed !!! \n\n")
