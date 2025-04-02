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

def create_bookings_table(connection: mysql.connector.MySQLConnection) -> bool:
    """Create the bookings table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id BIGINT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        apartment_id BIGINT NOT NULL,
        booking_date TIMESTAMP NOT NULL,
        checkin_date TIMESTAMP NOT NULL,
        checkout_date TIMESTAMP NOT NULL,
        total_price DECIMAL(10, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        booking_status VARCHAR(20) NOT NULL,
        INDEX idx_user_id (user_id),
        INDEX idx_apartment_id (apartment_id),
        INDEX idx_booking_date (booking_date),
        INDEX idx_check_in_date (checkin_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Bookings table created successfully")
            return True
    except Error as e:
        logger.error(f"Error creating table: {str(e)}")
        return False

def prepare_bookings_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare bookings data before insertion."""
    try:
        # Make a copy to avoid modifying the original dataframe
        df = df.copy()
        
        # Convert date columns to datetime
        date_columns = ['booking_date', 'checkin_date', 'checkout_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', dayfirst=True)
            # Convert to string format YYYY-MM-DD for MySQL
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        # Set all currency values to USD
        df['currency'] = 'USD'
        
        # Log the transformations
        logger.info("Date columns converted to YYYY-MM-DD format")
        logger.info(f"Currency values after modification: {df['currency'].unique()}")
        
        return df
    except Exception as e:
        logger.error(f"Error preparing data: {str(e)}")
        raise

def insert_bookings_batch(connection: mysql.connector.MySQLConnection, data: pd.DataFrame) -> bool:
    """Insert data in batches."""
    insert_query = """
        INSERT INTO bookings (
            booking_id, user_id, apartment_id, booking_date, 
            checkin_date, checkout_date, total_price, currency,
            booking_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

def main():
    """Main execution function."""
    try:
        # Load CSV file
        logger.info("Loading CSV file...")
        bookings_df = pd.read_csv("data/bookings.csv")
        
        # Log the column names and data types
        logger.info("DataFrame columns and types:")
        logger.info(bookings_df.dtypes)
        
        # Prepare data
        logger.info("Preparing data...")
        bookings_df = prepare_bookings_data(bookings_df)
        
        # Connect to database
        connection = connect_to_database()
        if not connection:
            return

        try:
            # Create table first
            if not create_bookings_table(connection):
                logger.error("Failed to create table. Aborting.")
                return

            # Insert data
            if insert_bookings_batch(connection, bookings_df):
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
