import pyspark
from delta import configure_spark_with_delta_pip
from library.file_parsers import load_export


if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("Lever2AWS")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the Lever export files into Delta tables
    load_export("/Users/kasztp/Documents/Lever_backup", "./temp/lever_imports")

    # Query the Delta tables
    jobs_df = spark.read.format("delta").load("./temp/lever_imports/lever_jobs")
    print(jobs_df.schema)
    candidates_df = spark.read.format("delta").load("./temp/lever_imports/lever_profile_cards")
    print(candidates_df.schema)

    jobs_df.show(5)

    candidates_df.show(5)

    spark.stop()
