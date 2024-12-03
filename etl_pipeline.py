# etl_pipeline_user_behavior.py

import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from db_connection import connect_to_db
from config import DATA_FOLDER

# Extract step: Read the CSV file
def extract_data(file_path):
    df = pd.read_csv(file_path)
    print(f"Data extracted successfully from {file_path}.")
    return df

# Transform step
def transform_data(df):
    # Handle missing data
    df.dropna(subset=['UserID'], inplace=True)  # Drop rows with missing UserID
    df.fillna({'AppUsageTime(min/day)': df['AppUsageTime(min/day)'].mean()}, inplace=True)  # Impute missing values

    # Standardize screen time to minutes (if needed)
    df['ScreenOnTime(min/day)'] = df['ScreenOnTime(hours/day)'] * 60

    # Calculate Battery Efficiency
    df['BatteryEfficiency(mAh/hour)'] = df['BatteryDrain(mAh/day)'] / df['ScreenOnTime(hours/day)']

    # Map UserBehaviorClass to labels
    class_mapping = {1: "Very Low", 2: "Low", 3: "Moderate", 4: "High", 5: "Very High"}
    df['UserBehaviorLabel'] = df['UserBehaviorClass'].map(class_mapping)

    # Round specific columns
    df['BatteryEfficiency(mAh/hour)'] = df['BatteryEfficiency(mAh/hour)'].round(2)
    df['ScreenOnTime(min/day)'] = df['ScreenOnTime(min/day)'].round(2)
    df['DataUsage(MB/day)'] = df['DataUsage(MB/day)'].round(2)

    # Rename columns to match SQL Server table
    df.rename(columns={
        'UserID': 'UserId',
        'DeviceModel': 'DeviceModel',
        'OperatingSystem': 'OperatingSystem',
        'AppUsageTime(min/day)': 'AppUsageTimeMinPerDay',
        'ScreenOnTime(min/day)': 'ScreenOnTimeMinPerDay',
        'BatteryDrain(mAh/day)': 'BatteryDrainPerDay',
        'NumberofAppsInstalled': 'AppsInstalledCount',
        'DataUsage(MB/day)': 'DataUsagePerDay',
        'Age': 'UserAge',
        'Gender': 'UserGender',
        'UserBehaviorClass': 'BehaviorClass',
        'UserBehaviorLabel': 'BehaviorLabel',
        'BatteryEfficiency(mAh/hour)': 'BatteryEfficiency'
    }, inplace=True)

    print(f"Transformed data for user behavior dataset.")
    return df

# Load step
def load_data_to_sql(df, engine):
    try:
        df.to_sql('UserBehaviorData', con=engine, if_exists='append', index=False)
        # Print the IDs of users added
        added_ids = df['UserId'].tolist()
        print(f"Data for the following User IDs has been added: {added_ids}")
    except SQLAlchemyError as e:
        print(f"Error occurred while inserting data: {e}")

# Main ETL function
def etl_pipeline():
    engine = connect_to_db()

    for file_name in os.listdir(DATA_FOLDER):
        if file_name.endswith(".csv"):  # Process only CSV files
            file_path = os.path.join(DATA_FOLDER, file_name)
            
            # Extract
            df = extract_data(file_path)
            
            # Transform
            df_transformed = transform_data(df)
            
            # Load
            load_data_to_sql(df_transformed, engine)

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()
