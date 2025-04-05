
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# RDS MySQL connection details
rds_jdbc_url = "jdbc:mysql://apartment-db.cl4s228oi3rd.eu-west-1.rds.amazonaws.com:3306/apartmentDB"
common_options = {
    "url": rds_jdbc_url,
    "user": "admin",
    "password": "Cha_rles123", 
    "customJdbcDriverS3Path": "s3://rds-s3-bkttt/connector/mysql-connector-j-8.4.0.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
}

# df = spark.read.format("jdbc").options(
#     url=rds_jdbc_url,
#     driver="com.mysql.cj.jdbc.Driver",
#     dbtable="apartment",
#     user="admin",
#     password="Cha_rles123"
# ).load()

# df.show(5)
# df.printSchema()

# tables_df = spark.read.format("jdbc").options(
#     url=rds_jdbc_url,
#     driver="com.mysql.cj.jdbc.Driver",
#     query="SHOW TABLES",
#     user="admin",
#     password="Cha_rles123"
# ).load()

# print("Tables in database:")
# tables_df.show()

# List of tables to extract
tables = ["apartment", "apartment_attributes", "user_viewings", "bookings"]
s3_base_path = "s3://rds-s3-bktt/loads_from_rds/"

for table in tables:
    print(f"\n🔹 Extracting data from table: {table}")

    # Extract data from MySQL table into a Spark DataFrame
    options = common_options.copy()
    options["dbtable"] = table

    spark_df = spark.read.format("jdbc").options(**options).load()

    # **DEBUG: Print schema and row count**
    print(f"Schema for {table}:")
    spark_df.printSchema()
    
    row_count = spark_df.count()
    print(f"Count for {table}: {row_count}")



    # Convert Spark DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_df")
    print(f"DynamicFrame Schema for {table}: {dynamic_frame.schema()}")

    # Define S3 path
    s3_path = f"{s3_base_path}{table}/"
    print(f"🔹 Writing {row_count} rows to {s3_path}")

    # Write DynamicFrame to S3 in Parquet format
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": s3_path},
        format="parquet"
    )


# Commit job
job.commit()
print("\n✅ Glue job completed successfully!")
