#################################################################################################
# Developed By: Mitch Philippe                                                                  #
# Developed Date: 3/1/2023                                                                      #
# Script NAME: copy_file_hdfs_to_local                                                          #
# PURPOSE: Copy input vendor files from local to HDFS.                                          #
#################################################################################################


# Declare a variable to hold the unix script name.
JOBNAME="copy_files_hdfs_to_local.sh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a Log File where logs would be generated
LOGFILE="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"


#################################################################################################
###   COMMENTS: From this point on, all  standard output and standard error will be logged in   #
###              the log file.                                                                  #
#################################################################################################


{  # <--- Start of the log file.
echo "${JOBNAME} Started ...: $(date)"
LOCAL_OUTPUT_PATH="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

HDFS_OUTPUT_PATH=PrescriberAnalytics/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

### Delete the files at Local paths if exists
rm -f ${LOCAL_CITY_DIR}/*
rm -f ${LOCAL_FACT_DIR}/*

### Copy the City and Fact file from HDFS to local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/

echo "${JOBNAME} is completed ...: $(date)"
}  > ${LOGFILE} 2>&1  # <---  End of program end log.
