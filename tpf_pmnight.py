import psycopg2
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(
    filename='update_status1.log',  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s'  
)

# Database connection configuration
db_config = {
    "host": "192.168.160.33",
    "port": "5433",
    "database": "verve",
    "user": "postgres",
    "password": "Avis!123"
}

connection = None  

try:
    logging.info("Starting the script to update list statuses.")

    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    logging.info("Connected to the database successfully.")

    # Define dates
    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    day_before_yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
    five_days_ago = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")  # 5 days old

    # Generate dynamic list names
    today_names = [f"Night_Google_{today}", f"Night_Facebook_{today}", f"Night_4attempt_{today}"]
    yesterday_names = [f"Night_Google_{yesterday}", f"Night_Facebook_{yesterday}", f"Night_4attempt_{yesterday}"]
    day_2_names = [f"Night_Google_{day_before_yesterday}", f"Night_Facebook_{day_before_yesterday}", f"Night_4attempt_{day_before_yesterday}"]
    five_days_old_names = [f"Night_Google_{five_days_ago}", f"Night_Facebook_{five_days_ago}", f"Night_4attempt_{five_days_ago}"]

    logging.info(f"Today names: {today_names}")
    logging.info(f"Yesterday names: {yesterday_names}")
    logging.info(f"Day-before-yesterday names: {day_2_names}")
    logging.info(f"Five-days-old names to delete: {five_days_old_names}")

    # Update today's and yesterday's lists to ACTIVE with priority and weightage
    cursor.execute("""
        UPDATE ct_list
        SET status = 'ACTIVE', priority = 9, weightage = 0.1
        WHERE name = ANY(%s)
    """, (today_names + yesterday_names,))
    logging.info("Updated today's and yesterday's lists to ACTIVE.")

    # Update day-2's lists to INACTIVE
    cursor.execute("""
        UPDATE ct_list
        SET status = 'INACTIVE'
        WHERE name = ANY(%s)
    """, (day_2_names,))
    logging.info("Updated day-before-yesterday's lists to INACTIVE.")

    # Delete lists that are 5 days old
    cursor.execute("""
        DELETE FROM ct_list
        WHERE name = ANY(%s)
    """, (five_days_old_names,))
    logging.info("Deleted lists older than 5 days.")

    # Commit the changes
    connection.commit()
    logging.info("Database changes committed successfully.")

except Exception as e:
    logging.error(f"An error occurred: {e}")

finally:
    if connection:
        connection.close()
        logging.info("Database connection closed.")
    else:
        logging.warning("No database connection to close.")
