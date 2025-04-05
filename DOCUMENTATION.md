# Rental Marketplace Analytics Pipeline Documentation

## Table of Contents
- [1. System Overview](#1-system-overview)
- [2. Technical Architecture](#2-technical-architecture)
- [3. Data Model](#3-data-model)
- [4. ETL Pipeline](#4-etl-pipeline)
- [5. Deployment & Configuration](#5-deployment--configuration)
- [6. Monitoring & Maintenance](#6-monitoring--maintenance)
- [7. Disaster Recovery](#7-disaster-recovery)

## 1. System Overview

### 1.1 Purpose
End-to-end data pipeline processing rental marketplace data for analytical reporting, transforming operational data from Aurora MySQL into a Redshift data warehouse.

### 1.2 Architecture Diagram
![System Architecture](images/architecture.png)

### 1.3 Key Components
- Source: AWS Aurora MySQL
- Data Lake: Amazon S3
- Warehouse: Amazon Redshift
- Processing: AWS Glue
- Orchestration: Step Functions
- Monitoring: CloudWatch

## 2. Technical Architecture

### 2.1 Infrastructure Components
```yaml
Source Database:
  Type: AWS Aurora MySQL
  Instance: db.r5.2xlarge
  Storage: 1TB

Data Lake:
  Service: Amazon S3
  Buckets:
    - raw-zone: s3://rental-analytics/raw/
    - processed-zone: s3://rental-analytics/processed/
    - curated-zone: s3://rental-analytics/curated/

Data Warehouse:
  Type: Amazon Redshift
  Node Type: ra3.xlplus
  Nodes: 2
```

### 2.2 Pipeline Flow
![Step Functions Workflow](images/step-functions-flow.png)

## 3. Data Model

### 3.1 Source Tables
```sql
-- Core tables structure
apartments (
    apartment_id INT PRIMARY KEY,
    location VARCHAR(255),
    price DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP
)

apartment_attributes (
    attribute_id INT PRIMARY KEY,
    apartment_id INT,
    attribute_name VARCHAR(100),
    attribute_value VARCHAR(255)
)

user_viewing (
    viewing_id INT PRIMARY KEY,
    user_id INT,
    apartment_id INT,
    viewing_date TIMESTAMP
)

bookings (
    booking_id INT PRIMARY KEY,
    apartment_id INT,
    user_id INT,
    start_date DATE,
    end_date DATE,
    total_amount DECIMAL(10,2)
)
```

### 3.2 Warehouse Schema
```sql
-- Dimensional model
dim_apartments (
    apartment_key INT IDENTITY(1,1),
    apartment_id INT,
    location_key INT,
    current_price DECIMAL(10,2),
    effective_date DATE,
    end_date DATE
)

fact_bookings (
    booking_key INT IDENTITY(1,1),
    booking_id INT,
    apartment_key INT,
    user_key INT,
    booking_date DATE,
    amount DECIMAL(10,2),
    duration_days INT
)
```

## 4. ETL Pipeline

### 4.1 Extraction Process (RDS_TO_S3_job)
```python
# Key transformation logic
def extract_rental_data():
    # Extract from Aurora MySQL
    df = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "apartments") \
        .load()
    
    # Apply transformations
    df = df.withColumn("load_date", current_date())
    
    # Write to S3
    df.write \
        .format("parquet") \
        .partitionBy("load_date") \
        .mode("append") \
        .save(s3_raw_path)
```

### 4.2 Loading Process (glue_to_redshift_job)
```python
# Redshift load logic
def load_to_redshift():
    # Quality checks
    df = apply_data_quality_rules(df)
    
    # Load to Redshift
    df.write \
        .format("redshift") \
        .option("url", redshift_url) \
        .option("dbtable", "dim_apartments") \
        .mode("append") \
        .save()
```

## 5. Deployment & Configuration

### 5.1 Environment Setup
```bash
# Required environment variables
export REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
export REDSHIFT_DATABASE=dev
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=your-password
export S3_BUCKET=your-data-lake-bucket
export GLUE_ROLE_ARN=arn:aws:iam::account:role/GlueETLRole
```

### 5.2 Deployment Steps
```bash
# Deploy infrastructure
terraform init
terraform apply

# Deploy Glue jobs
aws glue create-job \
    --name RDS_TO_S3_job \
    --role GlueETLRole \
    --command Name=glueetl,ScriptLocation=s3://scripts/extract.py

# Deploy Step Functions
aws stepfunctions create-state-machine \
    --name RentalETLPipeline \
    --definition file://step_function_flow.json
```

## 6. Monitoring & Maintenance

### 6.1 Key Metrics
```yaml
Pipeline Health:
  - Job Success Rate: >98%
  - Data Freshness: <24 hours
  - Error Rate: <1%

Data Quality:
  - Completeness: >99%
  - Accuracy: >99.9%
  - Consistency: >99%
```

### 6.2 Alert Configuration
```yaml
Critical Alerts:
  - Pipeline Failures
  - Data Quality Breaches
  - Latency >2 hours

Warning Alerts:
  - Processing Delays
  - Unusual Data Patterns
  - Resource Utilization >80%
```

## 7. Disaster Recovery

### 7.1 Backup Strategy
- S3 versioning enabled
- Redshift automated snapshots (daily)
- Cross-region replication for critical data

### 7.2 Recovery Procedures
1. **Pipeline Failure**
   ```bash
   # Retry failed step
   aws stepfunctions start-execution \
       --state-machine-arn $STATE_MACHINE_ARN \
       --input '{"retry": true, "failed_job": "RDS_TO_S3_job"}'
   ```

2. **Data Recovery**
   ```sql
   -- Restore from backup
   RESTORE TABLE dim_apartments FROM
   's3://backup/dim_apartments/2023-08-01/'
   ```

### 7.3 Failover Process
```yaml
Steps:
  1. Activate standby Redshift cluster
  2. Update DNS entries
  3. Redirect Glue jobs
  4. Verify data consistency
```

---
Last Updated: [Current Date]
Version: 1.0