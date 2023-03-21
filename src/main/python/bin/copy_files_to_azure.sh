#####################################################################################################
# Developed By: Mitch Philippe                                                                      #
# Developed Date: 03/02/2023                                                                        #
# Script Name: copy_files_to_azure.sh                                                               #
# PURPOSE: Copy input vendor files from local to HDFS.                                              #
#####################################################################################################


# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_azure.sh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

# Define a Log File where logs would be generated
LOGFILE="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"

######################################################################################################
### COMMENTS: From this point on, all standard output and standard error will                        #
###           be logged in the log file.                                                             #
######################################################################################################


{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

### Define Local Directories
LOCAL_OUTPUT_PATH="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

### Define SAS URLs
citySasUrl="https://prescriberanalytics.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=${city_key}"
prescSasUrl="https://prescriberanalytics.blob.core.windows.net/presc/${bucket_subdir_name}?st=${city_key}"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
