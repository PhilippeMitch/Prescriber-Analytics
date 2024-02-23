from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def column_split_cnt(column):
    return len(column.split(' '))
