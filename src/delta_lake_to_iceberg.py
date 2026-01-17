from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit

spark = SparkSession.builder.appName("NYC Taxi to Iceberg").getOrCreate()

taxi_types = ['for_hire_vehicle', 'green_taxi', 'high_volume_for_hire_vehicle', 'yellow_taxi']

try:
    # Create namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.nyc")

    for taxi_type in taxi_types:
        print(f"Processing {taxi_type}...")

        raw_path = f"s3a://nyc-taxi/Dataset/*/{taxi_type}/*.parquet"
        df = spark.read.option('mergeSchema', 'true').parquet(raw_path)

        df = df.withColumn("taxi_type", lit(taxi_type)) \
            .withColumn("file_year", regexp_extract(input_file_name(), r"Dataset/(\d{4})/", 1))

        # Write to Iceberg
        df.writeTo(f"iceberg.nyc.{taxi_type}") \
            .tableProperty("write.format.default", "parquet") \
            .partitionedBy("file_year") \
            .createOrReplace()

        print(f"✓ Completed {taxi_type}")

    print("All taxi types processed successfully!")

finally:
    spark.stop()