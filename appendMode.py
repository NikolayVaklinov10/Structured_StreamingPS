from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("SparkStreamingAppendMode")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("lsoa_code", StringType(), True),\
                         StructField("borough", StringType(), True),\
                         StructField("major_category", StringType(), True),\
                         StructField("minor_category", StringType(), True),\
                         StructField("value", StringType(),True),\
                         StructField("year", StringType(), True),\
                         StructField("month", StringType(), True)])

    fileStreamDF = sparkSession.readStream\
                               .option("header", "true")\
                               .schema(schema)\
                               .csv("datasets/droplocation")

    print(" ")
    print("Is the stream ready?")
    print(fileStreamDF.isStreaming)

    trimmedDF = fileStreamDF.select(
                                      fileStreamDF.borough,
                                      fileStreamDF.year,
                                      fileStreamDF.value
                                      )\
                            .withColumnRenamed(
                                    "value",
                                    "convictions"
                                    )

    query = trimmedDF.writeStream\
                     .outputMode("append")\
                     .format("console")\
                     .option("truncate", "false")\
                     .option("numRows", 30)\
                     .start()\
                     .awaitTermination()
