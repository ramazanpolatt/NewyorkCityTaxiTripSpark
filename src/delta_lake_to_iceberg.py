from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit

spark = SparkSession.builder.appName("NYC Taxi to Iceberg").getOrCreate()

taxi_types = ['for_hire_vehicle', 'green_taxi', 'high_volume_for_hire_vehicle', 'yellow_taxi']
# İşlemek istediğin yılları buraya ekle
years = [str(y) for y in range(2009, 2026)]

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.nyc")

    for taxi_type in taxi_types:
        print(f"--- Starting {taxi_type} ---")

        table_name = f"iceberg.nyc.{taxi_type}"
        first_run = True

        for year in years:
            raw_path = f"s3a://nyc-taxi/Dataset/{year}/{taxi_type}/*.parquet"

            try:

                df = spark.read.parquet(raw_path)


                if df.count() > 0:
                    print(f"Processing {taxi_type} for Year: {year}...")

                    df = df.withColumn("taxi_type", lit(taxi_type)) \
                        .withColumn("file_year", lit(year))

                    if first_run:

                        df.writeTo(table_name) \
                            .tableProperty("write.format.default", "parquet") \
                            .partitionedBy("file_year") \
                            .createOrReplace()
                        first_run = False
                    else:

                        df.writeTo(table_name).append()

                    print(f"✓ Year {year} appended.")

            except Exception as e:

                continue

        print(f"✓✓ {taxi_type} completely processed.")

finally:
    spark.stop()