import os

### Set Environment Variables
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

### Set Order Variables
appName = 'USA Prescriber Research Report'
currenPath = os.getcwd()

statging_dim_city = "PrescriberAnalytics/staging/dimension_city"
statging_fact = "PrescriberAnalytics/staging/fact"

output_city = "PrescriberAnalytics/output/dimension_city"
output_fact = "PrescriberAnalytics/output/presc"
