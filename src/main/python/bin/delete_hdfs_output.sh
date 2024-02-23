##############################################################################################
# Developed By: Philippe Mitch                                                               #
# Developed date : 2/28/2023                                                                 #
# Script Name: delete_hdfs_output_path                                                       #
# Purpose: Delete HDFS Output path so that Spark extraction will be smooth                   #
##############################################################################################

# Declare a variable to hold the unix script name.
JOBNAME="delete_hdfs_output_paths.sh"

# Declare a  variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a Log File where logs would be generated
LOGFILE='/home/hadoop/Projects/PrescriberAnalytics/src/main/python/logs/JOBNAME_${date}.log'


##############################################################################################
# COMMENTS: From this point on, all standard output and standard error will be logged in the #
#           log file.                                                                        #
##############################################################################################
{	# <--- Start of the log file.
echo '${JOBNAME} Started...: $(date)'
CITY_PATH=PrescriberAnalytics/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0]
  then
  echo 'The HDFS output directory $CITY_PATH is available. Proced to delete.'
  hdfs dfs -rm -r -f $CITY_PATH
  echo 'The HDFS Output directory $CITY_PATH is deleted before extraction.'
fi

FACT_PATH=PrescriberAnalytics/output/presc
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0]
  then
  echo 'The HDFS Output directory $FACT_PATH is available. Proced to delete.'
  hdfs dfs -rm -r -f $FACT_PATH
  echo 'The HDFS Output directory $FACT_PATH is deleted before extraction.'
fi

echo '${JOBNAME} is completed...: $(date)'

} > ${LOGFILE} 2>&1 # <---End of program and end of log
