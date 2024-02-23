###############################################################################################
# Developed By: Mitch Philippe                                                                #
# Developed Date: 03/02/2023                                                                  #
# Script Name: copy_files_to_s3.sh                                                            #
# PURPOSE: Copy input vendor files from local to HDFS.                                        #
###############################################################################################

# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_s3.sh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"

###############################################################################################
### COMMENTS: From this point on, all standard output and standard error will                 #
###           be logged in the log file.                                                      #
###############################################################################################


{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

LOCAL_OUTPUT_PATH="/home/hadoop/Projects/PrescriberAnalytics/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc


### Push City  and Fact files to s3.
for file in ${LOCAL_CITY_DIR}/*.*
do
  aws s3 --profile sparkprofile cp ${file} "s3://prescriber-analytics/dimension_city/$bucket_subdir_name/"
  echo "City File $file is pushed to S3."
done

for file in ${LOCAL_FACT_DIR}/*.*
do
  aws s3 --profile sparkprofile cp ${file} "s3://prescriber-analytics/presc/$bucket_subdir_name/"
  echo "Presc file $file is pushed to s3."
done

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
