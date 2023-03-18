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
citySasUrl="https://prescriberanalytics.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=2023-03-02T04:20:49Z&se=2023-03-02T12:20:49Z&si=cwlaccess&spr=https&sv=2021-06-08&sr=c&sig=yMuS7Cfu8jDLCUW88kTHd4x%2F2R%2FwDyLZYHa85%2FPV898%3D"
prescSasUrl="https://prescriberanalytics.blob.core.windows.net/presc/${bucket_subdir_name}?st=2023-03-02T04:23:02Z&se=2023-03-02T12:23:02Z&si=cwlaccess&spr=https&sv=2021-06-08&sr=c&sig=4n%2BWsEz%2FzrmY43d3PQrBkG0fYhbwKye0rSv0QdNLf50%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
