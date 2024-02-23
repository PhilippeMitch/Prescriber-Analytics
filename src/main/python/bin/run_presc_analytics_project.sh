#########################################################################################################
# Developed By: Mitch Philippe                                                                          #
# Developed date: 3/6/2023                                                                              #
# Script Name: run_presc_analitycs_project.sh                                                           #
# PURPOSE:                                                                                              #
#########################################################################################################
PROJ_FOLDER="/home/${USER}/Projects/PrescriberAnalytics/src/main/python"

#### Part 1
### Call the copy_to_hdfs wrapper to copy the file from the local to HDFS.
printf "\nCalling copy_files_local_to_hdfs.sh at `date + "%d/%m/%Y_%H:M:%S"` !!! \n"
${PROJ_FOLDER}/bin/copy_files_local_to_hdfs.sh
printf "Executing copy_files_local_to_hdfs.sh is completed at `date + "%d/%m%Y_%H:%M:%S"`!!! \n\n"

###  Call below wrapper to delete HDFS Paths.
printf "Calling delete_hdfs_output_paths.sh at `date + "%d/%m/%Y_%H:%M:%S"` !!! \n"
${PROJ_FOLDER}/bin/delete_hdfs_output_paths.sh
printf "Executing deletec_hdfs_output_paths.sh is completed at `date + "%d/%m/%Y_%H:%M:%S"` !!! \n"

### Call below Spark Job to extract Fact and City Files
printf "Calling run_presc_pipeline.py at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
spark3-submit --master yarn --num-executors 2 --jars ${PROJ_FOLDER}/lib/postgresql-42.3.5.jar run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"


#### Part 2
### Call below script to copy files from HDFS to local.
printf "Calling copy_files_hdfs_to_local.sh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_hdfs_to_local.sh
printf "Executing copy_files_hdfs_to_local.sh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below script to copy files to S3.
printf "Calling copy_files_to_s3.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_to_s3.sh
printf "Executing copy_files_to_s3.sh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below script to copy files to Azure.
printf "Calling copy_files_to_azure.sh ... \n"
${PROJ_FOLDER}/bin/copy_files_to_azure.sh
printf "Executing copy_files_to_azure.sh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"
