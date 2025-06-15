import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime


class CheckPostAnalytics:
    def __init__(self, host, port, user, password, database):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.mediator = self.connection.cursor()


    def get_top_10_drug_related_vehicles(self):
        query = """
        SELECT vehicle_number
        FROM stops
        WHERE drugs_related_stop = TRUE
        LIMIT 10;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_most_searched_vehicles(self):
        query = """
        SELECT vehicle_number, COUNT(*) AS search_count
        FROM stops
        WHERE search_conducted = TRUE
        GROUP BY vehicle_number
        ORDER BY search_count DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_highest_arrest_rate_by_age_group(self):
        query = """
        SELECT 
            d.age_group,
            ROUND(
                (COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END)::FLOAT / COUNT(*) * 100)::NUMERIC, 
                2
            ) AS arrest_rate
        FROM drivers d
        JOIN stops s ON d.vehicle_number = s.vehicle_number
        GROUP BY d.age_group
        ORDER BY arrest_rate DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_gender_distribution_by_country(self):
        query = """
        SELECT 
            s.country_name,
            d.driver_gender,
            COUNT(*) AS total_stops
        FROM drivers d
        JOIN stops s ON d.vehicle_number = s.vehicle_number
        GROUP BY s.country_name, d.driver_gender
        ORDER BY s.country_name, d.driver_gender;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_race_gender_highest_search_rate(self):
        query = """
        SELECT 
            d.driver_race,
            d.driver_gender,
            ROUND((
                COUNT(CASE WHEN s.search_conducted = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS search_rate_percent
        FROM drivers d
        JOIN stops s ON d.vehicle_number = s.vehicle_number
        GROUP BY d.driver_race, d.driver_gender
        ORDER BY search_rate_percent DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_peak_traffic_stop_time(self):
        query = """
        SELECT 
            CASE 
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 5 AND 11 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 17 AND 20 THEN 'Evening'
                ELSE 'Night'
            END AS time_of_day,
            COUNT(*) AS total_stops
        FROM stops
        GROUP BY time_of_day
        ORDER BY total_stops DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_average_stop_duration_by_violation(self):
        query = """
        SELECT 
            v.violation,
            ROUND(AVG(
                CASE s.stop_duration
                    WHEN '<5 Min' THEN 3
                    WHEN '6-15 Min' THEN 10
                    WHEN '16-30 Min' THEN 23
                    WHEN '30+ Min' THEN 35
                END
            ), 2) AS avg_duration_minutes
        FROM stops s
        JOIN violations v ON v.vehicle_number = s.vehicle_number
        GROUP BY v.violation
        ORDER BY avg_duration_minutes DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_arrest_rate_by_time_of_day(self):
        query = """
        SELECT 
            CASE 
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 5 AND 11 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 17 AND 20 THEN 'Evening'
                ELSE 'Night'
            END AS time_of_day,
            COUNT(*) AS total_stops,
            COUNT(CASE WHEN is_arrested = TRUE THEN 1 END) AS total_arrests,
            ROUND(
                (COUNT(CASE WHEN is_arrested = TRUE THEN 1 END)::FLOAT / COUNT(*) * 100)::NUMERIC, 
                2
            ) AS arrest_rate_percent
        FROM stops
        GROUP BY time_of_day
        ORDER BY arrest_rate_percent DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_violation_search_arrest_stats(self):
        query = """
        SELECT 
            v.violation, 
            COUNT(*) AS incident_count
        FROM violations v
        JOIN stops s ON s.vehicle_number = v.vehicle_number 
        WHERE s.search_conducted = TRUE OR s.is_arrested = TRUE
        GROUP BY v.violation
        ORDER BY incident_count DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_common_violations_under_25(self):
        query = """
        SELECT 
            v.violation, 
            COUNT(*) AS violation_count
        FROM violations v
        JOIN drivers d ON d.vehicle_number = v.vehicle_number 
        WHERE d.driver_age < 25
        GROUP BY v.violation
        ORDER BY violation_count DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_rarely_flagged_violations(self):
        query = """
        SELECT 
            v.violation,
            COUNT(CASE WHEN s.search_conducted = TRUE OR s.is_arrested = TRUE THEN 1 END) AS search_or_arrest_count
        FROM violations v
        JOIN stops s ON v.vehicle_number = s.vehicle_number
        GROUP BY v.violation
        ORDER BY search_or_arrest_count ASC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_country_with_highest_drug_related_rate(self):
        query = """
        SELECT 
            country_name,
            COUNT(*) AS total_stops,
            COUNT(CASE WHEN drugs_related_stop = TRUE THEN 1 END) AS drug_related_count,
            ROUND(
                (COUNT(CASE WHEN drugs_related_stop = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS drug_related_rate_percent
        FROM stops
        GROUP BY country_name
        ORDER BY drug_related_rate_percent DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()

    def get_arrest_rate_by_country_violation(self):
        query = """
        SELECT 
            s.country_name,
            v.violation,
            ROUND(
                (COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS arrest_rate_percent
        FROM stops s
        JOIN violations v ON s.vehicle_number = v.vehicle_number
        GROUP BY s.country_name, v.violation
        ORDER BY arrest_rate_percent DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_country_with_most_search_stops(self):
        query = """
        SELECT 
            country_name,
            COUNT(*) AS search_conducted_count
        FROM stops
        WHERE search_conducted = TRUE
        GROUP BY country_name
        ORDER BY search_conducted_count DESC
        LIMIT 1;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_yearly_stops_arrests_by_country(self):
        query = """
        SELECT 
            country_name,
            stop_year,
            total_stops,
            total_arrests,
            ROUND(
                ((total_arrests::FLOAT / total_stops) * 100)::NUMERIC, 2
            ) AS arrest_rate_percent,
            RANK() OVER (PARTITION BY stop_year ORDER BY total_arrests DESC) AS arrest_rank_in_year
        FROM (
            SELECT 
                country_name,
                EXTRACT(YEAR FROM stop_date)::INT AS stop_year,
                COUNT(*) AS total_stops,
                COUNT(CASE WHEN is_arrested = TRUE THEN 1 END) AS total_arrests
            FROM stops
            GROUP BY country_name, EXTRACT(YEAR FROM stop_date)
        ) AS yearly_stats
        ORDER BY stop_year, arrest_rank_in_year;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_violation_trends_by_age_race(self):
        query = """
        SELECT 
            d.age_group,
            d.driver_race,
            v.violation,
            COUNT(*) AS violation_count
        FROM drivers d
        JOIN (
            SELECT 
                vehicle_number,
                violation
            FROM violations
            WHERE violation IS NOT NULL
        ) v ON d.vehicle_number = v.vehicle_number
        GROUP BY d.age_group, d.driver_race, v.violation
        ORDER BY violation_count DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_time_period_analysis_of_stops(self):
        query = """
        SELECT 
            EXTRACT(YEAR FROM stop_date) AS year,
            EXTRACT(MONTH FROM stop_date) AS month,
            EXTRACT(HOUR FROM stop_time) AS hour,
            COUNT(*) AS total_stops
        FROM stops
        GROUP BY year, month, hour
        ORDER BY year, month, hour;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_high_search_arrest_violations(self):
        query = """
        SELECT 
            v.violation,
            ROUND(
                (COUNT(CASE WHEN s.search_conducted = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS search_rate_percent,

            ROUND(
                (COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS arrest_rate_percent,

            RANK() OVER (ORDER BY 
                COUNT(CASE WHEN s.search_conducted = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) DESC
            ) AS search_rank,

            RANK() OVER (ORDER BY 
                COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) DESC
            ) AS arrest_rank

        FROM violations v
        JOIN stops s ON v.vehicle_number = s.vehicle_number
        GROUP BY v.violation
        ORDER BY search_rank, arrest_rank;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_driver_demographics_by_country(self):
        query = """
        SELECT 
            s.country_name,
            ROUND(AVG(d.driver_age), 1) AS avg_driver_age,
            ROUND(
                (COUNT(CASE WHEN d.driver_gender = 'M' THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS male_percentage,
            ROUND(
                (COUNT(CASE WHEN d.driver_gender = 'F' THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS female_percentage,
            COUNT(DISTINCT d.driver_race) AS race_diversity
        FROM drivers d
        JOIN stops s ON d.vehicle_number = s.vehicle_number
        GROUP BY s.country_name
        ORDER BY avg_driver_age DESC;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_top_5_highest_arrest_violations(self):
        query = """
        SELECT 
            v.violation,
            COUNT(*) AS total_stops,
            COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END) AS total_arrests,
            ROUND(
                (COUNT(CASE WHEN s.is_arrested = TRUE THEN 1 END)::FLOAT 
                / COUNT(*) * 100)::NUMERIC, 2
            ) AS arrest_rate_percent
        FROM violations v
        JOIN stops s ON v.vehicle_number = s.vehicle_number
        GROUP BY v.violation
        HAVING COUNT(*) > 0
        ORDER BY arrest_rate_percent DESC
        LIMIT 5;
        """
        self.mediator.execute(query)
        return self.mediator.fetchall()
    
    def get_all_violations(self):
        self.mediator.execute("SELECT DISTINCT violation FROM violations ORDER BY violation;")
        return [row[0] for row in self.mediator.fetchall()]
    
    def validate_officer_credentials(self, username, password):
        query = """
            SELECT * FROM officers 
            WHERE username = %s AND password = %s
        """
        self.mediator.execute(query, (username, password))
        result = self.mediator.fetchone()
        return result  

    def insert_driver_data(self, vehicle_number, driver_gender, driver_age, age_group, driver_race):
        query = """
            INSERT INTO drivers (vehicle_number, driver_gender, driver_age, age_group, driver_race)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (vehicle_number, driver_gender, driver_age, age_group, driver_race)
        self.mediator.execute(query, values)
        self.connection.commit()

    def insert_stop_data(self, vehicle_number, stop_date, stop_time, stop_duration, country_name, drugs_related_stop, search_conducted, is_arrested, stop_outcome, added_by):
        query = """
            INSERT INTO stops (vehicle_number, stop_date, stop_time, stop_duration, country_name, drugs_related_stop, search_conducted, is_arrested, stop_outcome, added_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (vehicle_number, stop_date, stop_time, stop_duration, country_name, drugs_related_stop, search_conducted, is_arrested, stop_outcome, added_by)
        self.mediator.execute(query, values)
        self.connection.commit()

    def insert_violation_data(self, vehicle_number, violation_raw, violation):
        query = """
            INSERT INTO violations (vehicle_number, violation_raw, violation)
            VALUES (%s, %s, %s)
        """
        values = (vehicle_number, violation_raw, violation)
        self.mediator.execute(query, values)
        self.connection.commit()
