from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit

spark = SparkSession.builder.appName("Delta Lake to Iceberg").getOrCreate()

taxi_types = ['for_hire_vehicle', 'green_taxi', 'high_volume_for_hire_vehicle', 'yellow_taxi']

try:

    for taxi_type in taxi_types:
        # .option("mergeSchema", "true") is your best friend here.
        # It tells Spark: "If 2009 has 10 columns and 2023 has 15, make the final table 15 columns
        # and fill the missing ones with NULL for the 2009 data."
        raw_path = f"s3a://nyc-taxi/Dataset/*/{taxi_type}/*.parquet"
        df = spark.read.option('mergeSchema', 'true').parquet(raw_path)

        # Add metadata columns so we know where the data came from
        df = df.withColumn("taxi_type", lit(taxi_type)) \
            .withColumn("file_year", regexp_extract(input_file_name(), r"Dataset/(\d{4})/", 1))

        # Write to Iceberg
        # 'append' mode is useful if you want to run this for each taxi type into the same table,
        # OR create separate tables as shown below:

        df.writeTo(f"demo.nyc.{taxi_type}") \
            .tableProperty("write.format.default", "parquet") \
            .partitionedBy("file_year") \
            .createOrReplace()
finally:

    spark.stop()
