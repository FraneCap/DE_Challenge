import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, ForeignKey
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Supabase connection details
def etl_to_warehouse():
    load_dotenv()
    user = os.getenv("user")
    password = os.getenv("password")
    host = "aws-0-sa-east-1.pooler.supabase.com"
    port = "5432"
    database = "postgres"



    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    metadata = MetaData()

    # Create 'warehouse' schema
    logging.info("ETL process started.")
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

    # Definition of fact and dimention tables
    dim_patients = Table('DimPatients', metadata,
        Column('PatientCode', String, primary_key=True),
        Column('PatientName', String),
        Column('PhoneNumber', String),
        schema='warehouse'
    )

    dim_date = Table('DimDate', metadata,
        Column('Date', Date, primary_key=True),
        Column('Year', Integer),
        Column('Month', Integer),
        Column('Day', Integer),
        Column('Weekday', Integer),
        Column('Week', Integer),
        schema='warehouse'
    )

    fact_hospital_stays = Table('FactHospitalStays', metadata,
        Column('PatientCode', String, ForeignKey('warehouse.DimPatients.PatientCode')),
        Column('AdmissionDateTime', Date),
        Column('TotalStayCost', Float),
        Column('TotalTestCost', Float),
        schema='warehouse'
    )

    metadata.create_all(engine)

    # ETL Process for DimPatients
    logging.info("Starting ETL process for DimPatients.")
    df_patients = pd.read_sql("SELECT * FROM patient;", engine)
    df_patients.rename(columns={
        'patient_code': 'PatientCode',
        'patient_name': 'PatientName',
        'phone_number': 'PhoneNumber'
    }, inplace=True)
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE warehouse.\"DimPatients\" CASCADE;")
    df_patients.to_sql('DimPatients', con=engine, schema='warehouse', if_exists='append', index=False)
    logging.info("DimPatients ETL process completed.")

    # Process for DimDate
    logging.info("Starting process for DimDate.")
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    df_dates = pd.DataFrame({'Date': pd.date_range(start_date, end_date)})
    df_dates['Year'] = df_dates['Date'].dt.year
    df_dates['Month'] = df_dates['Date'].dt.month
    df_dates['Day'] = df_dates['Date'].dt.day
    df_dates['Weekday'] = df_dates['Date'].dt.weekday
    df_dates['Week'] = df_dates['Date'].dt.isocalendar().week
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE warehouse.\"DimDate\" CASCADE;")
    df_dates.to_sql('DimDate', con=engine, schema='warehouse', if_exists='append', index=False)
    logging.info("DimDate process completed.")

  
    # Stay costs
    logging.info("Starting ETL process for FactsTable.")
    query_stay_costs = """
    WITH DateRanges AS (
        SELECT
            a.patient_code,
            a.admission_datetime,
            a.discharge_datetime,
            generate_series(a.admission_datetime::date, a.discharge_datetime::date, '1 day'::interval) AS date
        FROM admission a
    ),
    Costs AS (
        SELECT
            dr.patient_code,
            dr.admission_datetime,
            COALESCE(SUM(s.price), 0) AS total_stay_cost
        FROM DateRanges dr
        JOIN stay_daily_cost s ON dr.date >= s.price_date_from
        GROUP BY dr.patient_code, dr.admission_datetime
    )
    SELECT 
        patient_code,
        admission_datetime,
        SUM(total_stay_cost) AS total_stay_cost
    FROM Costs
    GROUP BY patient_code, admission_datetime
    """
    df_stay_costs = pd.read_sql(query_stay_costs, engine)

    # Test costs
    query_test_costs = """
    SELECT 
        ta.patient_code, 
        ta.admission_datetime,
        SUM(tc.price) AS total_test_cost
    FROM test_admission ta
    JOIN test_cost tc ON ta.test_code = tc.test_code
    AND ta.test_datetime::date >= tc.price_date_from
    GROUP BY ta.patient_code, ta.admission_datetime
    """
    df_test_costs = pd.read_sql(query_test_costs, engine)

    df_costs = pd.merge(df_stay_costs, df_test_costs, on=['patient_code', 'admission_datetime'], how='outer').fillna(0)
    df_costs.rename(columns={
        'patient_code': 'PatientCode',
        'admission_datetime': 'AdmissionDateTime',
        'total_stay_cost': 'TotalStayCost',
        'total_test_cost': 'TotalTestCost'
    }, inplace=True)
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE warehouse.\"FactHospitalStays\" CASCADE;")
    df_costs.to_sql('FactHospitalStays', con=engine, schema='warehouse', if_exists='append', index=False)
    logging.info("FactHospitalStays ETL process completed.")
    logging.info("ETL process completed.")


    # Validation of dimension keys in fact tables
    unique_dim_patient_codes = set(df_patients['PatientCode'].unique())
    unique_fact_patient_codes = set(df_costs['PatientCode'].unique())
    invalid_patient_codes = unique_fact_patient_codes ^ unique_dim_patient_codes
    if invalid_patient_codes:
        print(f"Invalid PatientCodes found in FactHospitalStays: {invalid_patient_codes}")
    else:
        print("All PatientCodes in FactHospitalStays are valid.")

etl_to_warehouse()