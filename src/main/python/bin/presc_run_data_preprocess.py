import logging
import logging.config
from pyspark.sql.functions import (
    upper, lit, regexp_extract,
    col, concat_ws, count, when, isnan, coalesce, avg, round
)
from pyspark.sql.window import Window

logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

def perform_data_cleaning(df1, df2):
    ### Clean df_city DataFrame:
    # Select only required Columns
    # Apply Upper Function for columns - city, state and Country
    # Convert city, state and country fields to Upper Case
    try:
        logger.info(f"perform_data_cleaning() is started for df_city dataFrame !!! \n\n")
        df_city_sel = df1.select(
            upper(df1.city).alias("city"),
            df1.state_id,
            upper(df1.state_name).alias('state_name'),
            upper(df1.county_name).alias('country_name'),
            df1.population,
            df1.zips
        )
        ### Clean df_fact DataFrame
        logger.info(f"perform_data_clean() is started for df_fact dataframe !!! \n\n")
        df_fact_sel = df2.select(
            df2.npi.alias('presc_id'),
            df2.nppes_provider_last_org_name.alias('presc_lname'),
            df2.nppes_provider_first_name.alias('presc_fname'),
            df2.nppes_provider_city.alias('presc_city'),
            df2.nppes_provider_state.alias('presc_state'),
            df2.specialty_description.alias('presc_spclt'),
            df2.years_of_exp,
            df2.drug_name,
            df2.total_claim_count.alias('trx_cnt'),
            df2.total_day_supply,
            df2.total_drug_cost
        )

        ### Add a Country Field 'USA'
        logger.info(f"perform_data_cleaning() is started for df_fact dataFrame !!! \n\n")
        df_fact_sel = df_fact_sel.withColumn('country_name', lit('USA'))

        ### Clean years_of_exp
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))

        ### Convert the years_of_exp datatype from string to Number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        ### Combine First Name and Last Name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

        ### Check and clean all teh Null/Nan Values
        # df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()

        ### Delete the record where the Presc_ID is Null
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

        ### Delete the record where the DRUG_NAME is Null
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

        ### Impute TRX_CNT where it is null as avg of trx_cnt for prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn("trx_cnt", coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
        df_fact_sel = df_fact_sel.withColumn("trx_cnt", col("trx_cnt").cast("integer"))

    except Exception as exp:
        logger.error("Error in the method - spark_cur_date(), Please check the Stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info('perform_data_cleaning is completed !!! \n\n')

    return df_city_sel, df_fact_sel
