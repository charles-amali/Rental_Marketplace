import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'REDSHIFT_CONNECTION'])
redshift_connection = args['REDSHIFT_CONNECTION']

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Paths (Ideally pass these as arguments instead of hardcoding)
s3_paths = {
    "apartment": "s3://rds-s3-bktt/loads_from_rds/apartment/",
    "attributes": "s3://rds-s3-bktt/loads_from_rds/apartment_attributes/",
    "viewing": "s3://rds-s3-bktt/loads_from_rds/user_viewings/",
    "bookings": "s3://rds-s3-bktt/loads_from_rds/bookings/"
}

redshift_tables = {
    "apartment": "raw_schema.apartment",
    "attributes": "raw_schema.apartment_attributes",
    "viewing": "raw_schema.user_viewings",
    "bookings": "raw_schema.bookings"
}

try:
    for key, path in s3_paths.items():
        # Ensure path is not None or empty
        if not path or path.strip() == "":
            print(f" ERROR: S3 path for {key} is empty or null!")
            continue  
        
        print(f" Loading data from: '{path}'")  
        
        # Load Data from S3
        try:
            df = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                format="parquet",
                connection_options={"paths": [path]}
            )
            
            # Check if the DataFrame has records
            record_count = df.count()
            if record_count == 0:
                raise ValueError(f" No data found in {path}. Check if Parquet files exist.")
            
            print(f" Loaded {record_count} records from {key}")
        
        except Exception as load_error:
            print(f" Error loading {key} from {path}: {str(load_error)}")
            continue  # Skip writing this dataset
        
        # Write to Redshift
        try:
            glueContext.write_dynamic_frame.from_jdbc_conf(
                frame=df,
                catalog_connection=redshift_connection,
                connection_options={
                    "dbtable": redshift_tables[key],
                    "database": "dev",
                    "redshiftTmpDir": "s3://my-redshift-temp-bkt/temp-dir/" 
                },
                transformation_ctx=f"write_{key}"
            )
            print(f"Successfully written {key} to Redshift!")
        
        except Exception as write_error:
            print(f" Error writing {key} to Redshift: {str(write_error)}")
            continue  # Skip writing this dataset
    
    print(" All data loaded to Redshift!")
    job.commit()

except Exception as e:
    print(f" Error: {str(e)}")
    job.commit()
    sys.exit(1)


