import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

class traffic_stops:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        # psycopg2 connection
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.mediator = self.connection.cursor()

        # SQLAlchemy engine for DataFrame loading
        self.engine_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.engine_string)

    def load_and_clean_data(self, filepath):
        df = pd.read_csv(filepath)

        df.drop(columns=['driver_age_raw'], inplace=True)

        df['stop_date'] = pd.to_datetime(df['stop_date'])
        df['stop_time'] = pd.to_datetime(df['stop_time'], errors='coerce').dt.time
        df['country_name'] = df['country_name'].astype('category')
        df['driver_gender'] = df['driver_gender'].astype('category')
        df['driver_race'] = df['driver_race'].astype('category')
        df['violation_raw'] = df['violation_raw'].astype('category')
        df['violation'] = df['violation'].astype('category')
        df['stop_outcome'] = df['stop_outcome'].astype('category')
        df['stop_duration'] = df['stop_duration'].astype('category')

        df['age_group'] = pd.cut(df['driver_age'],
                                 bins=[0, 18, 30, 50, 70, 120],
                                 labels=['Teen', 'Young Adult', 'Adult', 'Middle Age', 'Senior'])

        df_drivers = df[['vehicle_number', 'driver_gender', 'driver_age', 'age_group', 'driver_race']].drop_duplicates()
        df_stops = df[['vehicle_number', 'search_type', 'stop_date', 'stop_time',
                       'stop_duration', 'country_name', 'drugs_related_stop',
                       'search_conducted', 'is_arrested', 'stop_outcome']].drop_duplicates()
        df_violations = df[['vehicle_number', 'violation_raw', 'violation']].drop_duplicates()

        return df_drivers, df_stops, df_violations

    def create_tables(self):
        self.mediator.execute("""
            CREATE TABLE IF NOT EXISTS officers (
                officer_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,  
                role TEXT CHECK (role IN ('admin', 'data_entry')) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("TABLE 'officers' created.")

        self.mediator.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                vehicle_number VARCHAR(20) PRIMARY KEY,
                driver_gender CHAR(1) CHECK (driver_gender IN ('M', 'F')),
                driver_age INT CHECK (driver_age BETWEEN 0 AND 120),
                age_group VARCHAR(20),
                driver_race VARCHAR(50)
            );
        """)
        print("TABLE 'drivers' created.")

        self.mediator.execute("""
            CREATE TABLE IF NOT EXISTS stops (
                vehicle_number VARCHAR(20) PRIMARY KEY,
                search_type VARCHAR(100),
                stop_date DATE NOT NULL,
                stop_time TIME NOT NULL,
                stop_duration VARCHAR(20),
                country_name VARCHAR(50),
                drugs_related_stop BOOLEAN DEFAULT FALSE,
                search_conducted BOOLEAN DEFAULT FALSE,
                is_arrested BOOLEAN DEFAULT FALSE,
                stop_outcome VARCHAR(50),
                added_by TEXT REFERENCES officers(officer_id),
                FOREIGN KEY (vehicle_number) REFERENCES drivers(vehicle_number) ON DELETE CASCADE
            );
        """)
        print("TABLE 'stops' created.")

        self.mediator.execute("""
            CREATE TABLE IF NOT EXISTS violations (
                vehicle_number VARCHAR(20) PRIMARY KEY,
                violation_raw VARCHAR(100),
                violation VARCHAR(50),
                FOREIGN KEY (vehicle_number) REFERENCES stops(vehicle_number) ON DELETE CASCADE
            );
        """)
        print("TABLE 'violations' created.")


    def insert_sample_officers(self):
        self.mediator.execute("""
            INSERT INTO officers (officer_id, name, username, password, role)
            VALUES 
            ('A1','Admin Officer', 'admin1', 'adminpass', 'admin'),
            ('A2', 'Data Entry Officer', 'entry1', 'entrypass', 'data_entry')
            ON CONFLICT (username) DO NOTHING;
        """)


    def insert_data(self, df_drivers, df_stops, df_violations):
        df_drivers.to_sql('drivers', self.engine, if_exists='append', index=False)
        df_stops.to_sql('stops', self.engine, if_exists='append', index=False)
        df_violations.to_sql('violations', self.engine, if_exists='append', index=False)
        print("Data inserted successfully.")

    def close(self):
        self.mediator.close()
        self.connection.close()

def main():
    # Config
    host = "localhost"
    port = 5432
    user = "postgres"
    password = "vGpostgre"
    database = "traffic_stops"
    filepath = "/Users/Viji/Desktop/Guvi_python/MDTE21/guvi_projects/traffic_stops - traffic_stops_with_vehicle_number.csv"

    # Create instance
    app = traffic_stops(host, port, user, password, database)

    # Step 1: Load + Clean Data
    df_drivers, df_stops, df_violations = app.load_and_clean_data(filepath)

    # Step 2: Create Tables
    app.create_tables()

    # Step 3: Insert Dummy officer Data
    app.insert_sample_officers() 

    # Step 4: Insert Data
    app.insert_data(df_drivers, df_stops, df_violations)

    # Close
    app.close()


if __name__ == "__main__":
    main()
