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

def create_apartment_attributes_table(connection: mysql.connector.MySQLConnection) -> bool:
    """Create the apartment_attributes table if it doesn't exist."""
    # First check if apartment table exists
    check_apartment_table_query = """
    SELECT COUNT(*)
    FROM information_schema.tables 
    WHERE table_schema = %s 
    AND table_name = 'apartment'
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(check_apartment_table_query, (os.getenv("APARTMENT_DB_NAME"),))
            if cursor.fetchone()[0] == 0:
                logger.error("Base apartment table does not exist. Please run apartment_table.py first.")
                return False
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS apartment_attributes (
                apartment_id BIGINT NOT NULL,
                category VARCHAR(50),
                body TEXT,
                amenities TEXT,
                bathrooms DECIMAL(3,1),
                bedrooms DECIMAL(3,1),
                fee VARCHAR(100),
                has_photo VARCHAR(50),
                pets_allowed VARCHAR(50),
                price_display VARCHAR(50),
                price_type VARCHAR(20),
                square_feet INT,
                address VARCHAR(255),
                cityname VARCHAR(100),
                state CHAR(2),
                latitude DECIMAL(10, 4),
                longitude DECIMAL(10, 4),
                PRIMARY KEY (apartment_id),
                INDEX idx_category (category),
                INDEX idx_cityname (cityname),
                INDEX idx_state (state),
                INDEX idx_bedrooms (bedrooms),
                INDEX idx_bathrooms (bathrooms),
                INDEX idx_square_feet (square_feet),
                INDEX idx_location (latitude, longitude),
                FOREIGN KEY (apartment_id) REFERENCES apartment(id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Apartment attributes table created successfully")
            return True
    except Error as e:
        logger.error(f"Error creating table: {str(e)}")
        return False

def insert_apartment_attributes_batch(connection: mysql.connector.MySQLConnection, data: pd.DataFrame) -> bool:
    """Insert data in batches."""
    # Rename 'id' column to 'apartment_id' if it exists
    if 'id' in data.columns:
        data = data.rename(columns={'id': 'apartment_id'})

    insert_query = """
        INSERT INTO apartment_attributes (
            apartment_id, category, body, amenities, bathrooms, bedrooms,
            fee, has_photo, pets_allowed, price_display, price_type,
            square_feet, address, cityname, state, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples
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

def main():
    """Main execution function."""
    try:
        # Load CSV file
        logger.info("Loading CSV file...")
        attributes_df = pd.read_csv("data/apartment_attributes.csv")
        
        # Connect to database
        connection = connect_to_database()
        if not connection:
            return

        try:
            # Create table first
            if not create_apartment_attributes_table(connection):
                logger.error("Failed to create table. Aborting.")
                return

            # Insert data
            if insert_apartment_attributes_batch(connection, attributes_df):
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

