import psycopg2
import csv
from psycopg2 import sql

def create_database():
    try:
        # Step 1: Connect to default postgres DB
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="monica0808",  
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Step 2: Create new database
        db_name = "police_secure_logs"
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error creating database:", e)

def create_table():
    try:
        # Step 3: Connect to the new database
        conn = psycopg2.connect(
            dbname="police_secure_logs",
            user="postgres",
            password="monica0808",  
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Step 4: Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS police_checks_log (
            id SERIAL PRIMARY KEY,
            stop_date DATE,
            stop_time TIME,
            country_name VARCHAR(50),
            driver_gender VARCHAR(10),
            driver_age_raw INT,
            driver_age INT,
            driver_race VARCHAR(50),
            violation_raw VARCHAR(100),
            violation VARCHAR(50),
            search_conducted BOOLEAN,
            search_type VARCHAR(100),
            stop_outcome VARCHAR(50),
            is_arrested BOOLEAN,
            drugs_related_stop BOOLEAN,
            stop_duration VARCHAR(50),
            vehicle_number VARCHAR(20),
            timestamp TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'police_checks_log' created successfully.")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error creating table:", e)

def load_csv_data(filepath="traffic_stops.csv"):
    try:
        conn = psycopg2.connect(
            dbname="police_secure_logs",
            user="postgres",
            password="monica0808",  
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        with open(filepath, 'r') as file:
            reader = csv.reader(file)
            next(reader)  

            for row in reader:
                cursor.execute("""
                INSERT INTO police_checks_log (
                    stop_date, stop_time, country_name, driver_gender,
                    driver_age_raw, driver_age, driver_race,
                    violation_raw, violation, search_conducted,
                    search_type, stop_outcome, is_arrested,
                    stop_duration, drugs_related_stop, vehicle_number
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, row)

        conn.commit()
        print("Data loaded successfully from CSV.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error loading CSV data:", e)

if __name__ == "__main__":
    create_database()
    create_table()
    load_csv_data()  

