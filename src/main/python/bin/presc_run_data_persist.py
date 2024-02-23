import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config

logger = logging.getLogger(__name__)

def data_persist_hive(spark, df, dfName, partitionBy, mode):
    try:
        logger.info(f"Data persist Hive Script - data_persist() is started for saving dataframe " + dfName + " into Hive Table !!! \n\n")
        # Add a static column with the current date
        df = df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescriberanalytics location 'hdfs://localhost:9000/user/hive/warehouse/prescriberanalytics.db' """)
        spark.sql(""" use prescriberanalytics """)
        df.write.saveAsTable(dfName, mode = mode, partitionBy='delivery_date')
        #df.write.mode(mode).partitionBy('delivery_date').saveAsTable(dfName)
    except Exception as exp:
        logger.error("Error in the method - data_persist_hive(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Data Persist - data_persist_hive() is completed for saving dataframe " + dfName + " into Hive Table !!! \n\n")



def data_persist_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Data Persist Postgre Script - data_persist_postgre() is started for saving  dataframe " + dfName + " into Postgre Table !!! \n\n")
        df.write.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", dbtable) \
        .mode(mode) \
        .option("user", user) \
        .option("password", password) \
        .save()
    except Exception as exp:
        logger.error("Error in the method - data_persist_postgre(). Please check the Stack Tree. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Data Persist Postgre  Script - data_persist_postgre() is completed for saving dataframe " + dfName + " into Postgres Table !!! \n\n")
