import logging
import logging.config

### Load the Logging Configuration File
logging.config.fileConfig(fname="../util/logging_to_file.conf")

### Get the custom Logger from Configuration File
logger = logging.getLogger(__name__)

def load_files(spark, file_dir, file_format, header, inferschema):
    try:
        logger.info("The load_files() Function is started !!! \n\n")
        if file_format == 'parquet':
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv':
            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferschema=inferschema). \
                load(file_dir)
    except Exception as exp:
        logger.error("An error has occured in the method - load_files() - Please checkk the Stack Trace.")
        raise
    else:
        logger.info(f"The Input File {file_dir} is loaded to the data frame. The load_files() Funtion is completed !!! \n\n")

    return df
