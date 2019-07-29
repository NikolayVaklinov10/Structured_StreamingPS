from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

if __name__ == '__main__':

    sparkSession = SparkSession\
        .builder\
        .appName("Join")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    personal_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
                                          StructField('Gender', StringType(),True),\
                                          StructField('Age', StringType(), True)])

    customerDF = sparkSession.read\
                            .format("csv")\
                            .option("header", "true")\
                            .schema(personal_details_schema)\
                            .load("datasets/customerDatasets/static_datasets/join_static_personal_details.csv")

    transaction_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
                                             StructField('Transaction_amount', StringType(), True),\
                                             StructField('Transaction_Rating', StringType(), True)])

    fileStreamDf = sparkSession.readStream\
                            .option("header", "true")\
                            .option("maxFilesPerTrigger", 1)\
                            .schema(transaction_details_schema)\
                            .csv("datasets/customerDatasets/static_datasets/join_static_personal_details.csv")

    joinedDF = customerDF.join(fileStreamDf, "Customer_ID")
# an error raiseed on line 40 , needs to be fixed
    query = joinedDF\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .start()\
        .awaitTermination()
















