import psycopg2
import pandas as pd

# Configure PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    dbname="police_secure_logs",
    user="postgres",
    password="monica0808",
    port=5432
)
cursor = conn.cursor()

def insert_log(stop_date, stop_time, country, driver_gender, driver_age,
               driver_race, search_conducted, search_type, stop_outcome,
               stop_duration, is_drug_related, violation, vehicle_number):

    cursor.execute('''
        INSERT INTO police_checks_log (
            stop_date, stop_time, country_name, driver_gender, driver_age,
            driver_race, search_conducted, search_type, stop_outcome,
            stop_duration, drugs_related_stop, violation, vehicle_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        stop_date, stop_time, country, driver_gender, driver_age,
        driver_race, search_conducted, search_type, stop_outcome,
        stop_duration, is_drug_related, violation, vehicle_number
    ))
    conn.commit()

def run_custom_query(query):
    df = pd.read_sql_query(query, conn)
    return df

QUERY_MAP = {
    #Vehicle-Based
    "Top 10 Vehicles in Drug-Related Stops": """
        SELECT vehicle_number, COUNT(*) AS stop_count
        FROM police_checks_log
        WHERE drugs_related_stop = TRUE
        GROUP BY vehicle_number
        ORDER BY stop_count DESC
        LIMIT 10;
    """,

    "Most Frequently Searched Vehicles": """
        SELECT vehicle_number, COUNT(*) AS search_count
        FROM police_checks_log
        WHERE search_conducted = TRUE
        GROUP BY vehicle_number
        ORDER BY search_count DESC
        LIMIT 10;
    """,

    #Demographic-Based
    "Driver Age Group with Highest Arrest Rate": """
        SELECT age_group,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrests,
        ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM (
            SELECT *,
                CASE 
                    WHEN driver_age < 18 THEN 'Under 18'
                    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
                    WHEN driver_age BETWEEN 46 AND 60 THEN '46-60'
                    ELSE '60+' 
                END AS age_group
            FROM police_checks_log
            WHERE driver_age IS NOT NULL
        ) AS grouped_data
        GROUP BY age_group
        ORDER BY arrest_rate_percentage DESC
        LIMIT 1;
    """,

    "Gender Distribution by Country": """
        SELECT  country_name,
                driver_gender,
                COUNT(*) AS stop_count
        FROM police_checks_log
        WHERE driver_gender IS NOT NULL
        GROUP BY country_name, driver_gender
        ORDER BY country_name, stop_count DESC;

    """,

    "Race and Gender Combination with Highest Search Rate": """
        SELECT driver_race,
        driver_gender,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS searches,
        ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percentage
        FROM police_checks_log
        WHERE driver_race IS NOT NULL AND driver_gender IS NOT NULL
        GROUP BY driver_race, driver_gender
        ORDER BY search_rate_percentage DESC;

    """,

    #Time & Duration Based
    "Time of Day with Most Stops": """
        SELECT time_of_day,
        COUNT(*) AS stop_count
        FROM (
            SELECT *,
                CASE 
                    WHEN CAST(stop_time AS TIME) BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
                    WHEN CAST(stop_time AS TIME) BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
                    WHEN CAST(stop_time AS TIME) BETWEEN '18:00:00' AND '21:59:59' THEN 'Evening'
                    ELSE 'Night'
                END AS time_of_day
            FROM police_checks_log
            WHERE stop_time IS NOT NULL
        ) AS time_buckets
        GROUP BY time_of_day
        ORDER BY stop_count DESC;
    """,

    "Average Stop Duration per Violation": """
        SELECT violation,
        ROUND(AVG(
           CASE stop_duration
               WHEN '0-15 Min' THEN 7.5
               WHEN '16-30 Min' THEN 23
               WHEN '30+ Min' THEN 35
               ELSE NULL
           END
       ), 2) AS avg_stop_duration_minutes
        FROM police_checks_log
        WHERE stop_duration IS NOT NULL
        GROUP BY violation
        ORDER BY avg_stop_duration_minutes DESC;
    """,

    "Night Stops and Arrest Likelihood": """
        SELECT time_of_day,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrests,
        ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM (
                SELECT *,
                CASE 
                    WHEN CAST(stop_time AS TIME) BETWEEN '06:00:00' AND '21:59:59' THEN 'Day'
                    ELSE 'Night'
                END AS time_of_day
                FROM police_checks_log
                WHERE stop_time IS NOT NULL
            ) AS time_classified
        GROUP BY time_of_day
        ORDER BY arrest_rate_percentage DESC;
    """,

    #Violation-Based
    "Violations Associated with Searches or Arrests": """
        SELECT violation,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS searches,
        ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percentage,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrests,
        ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM police_checks_log
        WHERE violation IS NOT NULL
        GROUP BY violation
        ORDER BY search_rate_percentage DESC, arrest_rate_percentage DESC;
    """,

    "Most Common Violations Among Young Drivers": """
        SELECT violation,
        COUNT(*) AS stop_count
        FROM police_checks_log
        WHERE driver_age < 25 AND violation IS NOT NULL
        GROUP BY violation
        ORDER BY stop_count DESC;

    """,

    "Violations Rarely Resulting in Search or Arrest": """
        SELECT violation,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
        ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percentage,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
        ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM police_checks_log
        WHERE violation IS NOT NULL
        GROUP BY violation
        HAVING search_rate_percentage < 5 AND arrest_rate_percentage < 5
        ORDER BY total_stops DESC;

    """,

    #Location-Based
    "Countries with Highest Drug-Related Stops": """
        SELECT country_name,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) AS drug_related_stops,
        ROUND(SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS drug_related_rate_percentage
        FROM police_checks_log
        WHERE country_name IS NOT NULL
        GROUP BY country_name
        ORDER BY drug_related_rate_percentage DESC;
    """,

    "Arrest Rate by Country and Violation": """
        SELECT country_name,
        violation,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
        ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM police_checks_log
        WHERE country_name IS NOT NULL AND violation IS NOT NULL
        GROUP BY country_name, violation
        ORDER BY arrest_rate_percentage DESC;

    """,

    "Country with Most Stops and Search Conducted": """
        SELECT country_name,
        COUNT(*) AS search_conducted_count
        FROM police_checks_log
        WHERE search_conducted = TRUE AND country_name IS NOT NULL
        GROUP BY country_name
        ORDER BY search_conducted_count DESC
        LIMIT 1;
    """,

    # Complex Queries
    "Yearly Breakdown of Stops and Arrests by Country": """
        WITH yearly_stats AS (
            SELECT 
                country_name,
                EXTRACT(YEAR FROM stop_date) AS stop_year,
                COUNT(*) AS total_stops,
                SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
            FROM police_checks_log
            WHERE stop_date IS NOT NULL AND country_name IS NOT NULL
            GROUP BY country_name, EXTRACT(YEAR FROM stop_date)
        )
        SELECT *,
            ROUND(total_arrests * 100.0 / total_stops, 2) AS arrest_rate_percentage,
            SUM(total_stops) OVER (PARTITION BY country_name ORDER BY stop_year) AS cumulative_stops,
            SUM(total_arrests) OVER (PARTITION BY country_name ORDER BY stop_year) AS cumulative_arrests
        FROM yearly_stats
        ORDER BY country_name, stop_year;
    """,

    "Driver Violation Trends by Age and Race": """
        WITH age_grouped AS (
            SELECT *,
                CASE 
                    WHEN driver_age < 18 THEN 'Under 18'
                    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN driver_age BETWEEN 36 AND 50 THEN '36-50'
                    WHEN driver_age > 50 THEN '50+'
                    ELSE 'Unknown'
                END AS age_group
            FROM police_checks_log
            WHERE driver_age IS NOT NULL AND driver_race IS NOT NULL AND violation IS NOT NULL
        )
        -- Main query: count violations by age group and race
        SELECT ag.age_group,
            ag.driver_race,
            ag.violation,
            COUNT(*) AS violation_count
        FROM age_grouped ag
        GROUP BY ag.age_group, ag.driver_race, ag.violation
        ORDER BY ag.age_group, ag.driver_race, violation_count DESC;
    """,

    "Stop Trends by Year, Month, Hour": """
        WITH time_info AS (
            SELECT 
                stop_date,
                stop_time,
                EXTRACT(YEAR FROM stop_date) AS stop_year,
                EXTRACT(MONTH FROM stop_date) AS stop_month,
                EXTRACT(HOUR FROM CAST(stop_time AS TIME)) AS stop_hour
            FROM police_checks_log
            WHERE stop_date IS NOT NULL AND stop_time IS NOT NULL
        )
        SELECT 
            stop_year,
            stop_month,
            stop_hour,
            COUNT(*) AS stop_count
        FROM time_info
        GROUP BY stop_year, stop_month, stop_hour
        ORDER BY stop_year, stop_month, stop_hour;
    """,

    "Violations with High Search and Arrest Rates": """
        WITH violation_stats AS (
            SELECT 
                violation,
                COUNT(*) AS total_stops,
                SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
                SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
            FROM police_checks_log
            WHERE violation IS NOT NULL
            GROUP BY violation
        ),

        ranked_stats AS (
            SELECT *,
                ROUND(total_searches * 100.0 / total_stops, 2) AS search_rate,
                ROUND(total_arrests * 100.0 / total_stops, 2) AS arrest_rate,
                RANK() OVER (ORDER BY (total_searches * 1.0 / total_stops) DESC) AS search_rank,
                RANK() OVER (ORDER BY (total_arrests * 1.0 / total_stops) DESC) AS arrest_rank
            FROM violation_stats
        )
        SELECT 
            violation,
            total_stops,
            total_searches,
            search_rate,
            search_rank,
            total_arrests,
            arrest_rate,
            arrest_rank
        FROM ranked_stats
        WHERE search_rank <= 3 OR arrest_rank <= 3
        ORDER BY search_rank, arrest_rank;
    """,

    "Driver Demographics by Country": """
        SELECT 
            country_name,
            COUNT(*) AS total_stops,
            ROUND(AVG(driver_age), 1) AS avg_driver_age,
            COUNT(CASE WHEN driver_gender = 'M' THEN 1 END) AS male_drivers,
            COUNT(CASE WHEN driver_gender = 'F' THEN 1 END) AS female_drivers,
            COUNT(CASE WHEN driver_race = 'White' THEN 1 END) AS white_drivers,
            COUNT(CASE WHEN driver_race = 'Black' THEN 1 END) AS black_drivers,
            COUNT(CASE WHEN driver_race = 'Hispanic' THEN 1 END) AS hispanic_drivers,
            COUNT(CASE WHEN driver_race = 'Asian' THEN 1 END) AS asian_drivers,
            COUNT(CASE WHEN driver_race = 'Other' THEN 1 END) AS other_race_drivers
        FROM police_checks_log
        WHERE country_name IS NOT NULL
        GROUP BY country_name
        ORDER BY total_stops DESC;
    """,

    "Top 5 Violations with Highest Arrest Rates": """
        SELECT 
            violation,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
            ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percentage
        FROM police_checks_log
        WHERE violation IS NOT NULL
        GROUP BY violation
        HAVING COUNT(*) > 0
        ORDER BY arrest_rate_percentage DESC
        LIMIT 5;
    """
}
