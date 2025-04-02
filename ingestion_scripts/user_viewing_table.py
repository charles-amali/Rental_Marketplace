import os
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def connect_to_database() -> Optional[mysql.connector.MySQLConnection]:
    """Establish database connection."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("APARTMENT_DB_NAME")
        )
        if connection.is_connected():
            logger.info("Connected to MySQL database")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        return None

def create_user_viewings_table(connection: mysql.connector.MySQLConnection) -> bool:
    """Create the user_viewings table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_viewings (
        viewing_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        apartment_id BIGINT NOT NULL,
        viewed_at TIMESTAMP NOT NULL,
        is_wishlisted CHAR(1) NOT NULL,
        call_to_action VARCHAR(20) NOT NULL,
        INDEX idx_user_apartment (user_id, apartment_id),
        INDEX idx_apartment_id (apartment_id),
        INDEX idx_viewed_at (viewed_at),
        INDEX idx_user_id (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("User viewings table created successfully")
            return True
    except Error as e:
        logger.error(f"Error creating table: {str(e)}")
        return False

def insert_user_viewings_batch(connection: mysql.connector.MySQLConnection, data: pd.DataFrame) -> bool:
    """Insert data in batches."""
    insert_query = """
        INSERT INTO user_viewings (
            user_id, apartment_id, viewed_at, is_wishlisted, call_to_action
        ) VALUES (%s, %s, %s, %s, %s)
    """

    records = [tuple(x) for x in data.astype(object).where(pd.notnull(data), None).values]
    batch_size = 1000

    try:
        with connection.cursor() as cursor:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
                logger.info(f"Inserted {i + len(batch)} records so far...")
        logger.info("All records inserted successfully!")
        return True
    except Error as e:
        logger.error(f"Failed to insert batch: {str(e)}")
        connection.rollback()
        return False

def prepare_user_viewings_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare user viewings data before insertion."""
    try:
        # Make a copy to avoid modifying the original dataframe
        df = df.copy()
        
        # Convert viewed_at to datetime with explicit format
        df['viewed_at'] = pd.to_datetime(df['viewed_at'], format='%d/%m/%Y', dayfirst=True)
        
        # Keep all records but sort them by user_id, apartment_id, and viewed_at
        df = df.sort_values(['user_id', 'apartment_id', 'viewed_at'])
        
        # Convert to string format YYYY-MM-DD HH:MM:SS for MySQL
        df['viewed_at'] = df['viewed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Total number of viewing records: {len(df)}")
        logger.info("Datetime column 'viewed_at' converted to YYYY-MM-DD HH:MM:SS format")
        
        return df
    except Exception as e:
        logger.error(f"Error preparing data: {str(e)}")
        raise

def main():
    """Main execution function."""
    try:
        # Load CSV file
        logger.info("Loading CSV file...")
        viewings_df = pd.read_csv("data/user_viewing.csv")
        
        # Prepare data
        logger.info("Preparing data...")
        viewings_df = prepare_user_viewings_data(viewings_df)
        
        # Connect to database
        connection = connect_to_database()
        if not connection:
            return

        try:
            # Create table first
            if not create_user_viewings_table(connection):
                logger.error("Failed to create table. Aborting.")
                return

            # Insert data
            if insert_user_viewings_batch(connection, viewings_df):
                logger.info("Data insertion completed successfully")
            else:
                logger.error("Data insertion failed")
        finally:
            connection.close()
            logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
