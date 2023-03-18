import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

def extract_files(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info(f"Extraction - extract_files() is started !!! \n\n")
        df.coalesce(split_no) \
          .write \
          .format(format) \
          .save(filePath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error in the method - extract_files(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Extraction - extract_files() is completed !!!\n\n")
