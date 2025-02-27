#This file reads data from csv files, does some validations and then inserts it into db tables.
import pandas as pd
import logging
from api.database import Actual_session, Base, engine
from api.models import Department, Job, HiredEmployee



# Configure logging to write errors to a file named "error_data.log".
logging.basicConfig(
    filename="data_error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_tables():
    """
    Create tables in the database if they do not exist.
    """
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")


def nan_errors(row, index):
    """
    Logging NaN errors in DataFrame
    """
    for col in row.index:
        if pd.isna(row[col]):  
            logging.error(f"Row {index + 1} - Column {col} with NaN elements")


def validate_data(df, table_name):
    """
    Validate data in the DataFrame before inserting it into the database.
    Data should be in the correct format and not contain any null values.
    """

    try:
        if table_name =='deparments':
            #df = df.astype({'department_id': 'int64', 'department_name': 'str'})
            df['id'] = pd.to_numeric(df['id'], errors='coerce', downcast='integer')
            df['department'] = df['department'].astype(str)
        
        elif table_name == 'jobs':
            df['id'] = pd.to_numeric(df['id'], errors='coerce', downcast='integer')
            df['job'] = df['job'].astype(str)

        elif table_name == 'hired_employees':
            df['id'] = pd.to_numeric(df['id'], errors='coerce', downcast='integer')
            df['name'] = df['name'].astype(str)
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce', downcast='integer')
            df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce', downcast='integer')

        # if df.isnull().values.any():
        #     errors.append("Data contains null values.")
        #     df = df.dropna()
    except Exception as e:
        logging.error(f"Error validating data from {table_name}: {e}")
        return pd.DataFrame(), [f"Error validating data from {table_name}: {e}"]
    
    return df


def load_data(csv_path, model, table_name, colums):
    """
    Load data from a CSV file into the database.
    """
    try:
        df = pd.read_csv(csv_path, header = None, names=colums)
    except Exception as e:
        logging.error(f"Error loading data from {csv_path}: {e}")
        return
    
    df = validate_data(df, table_name)
    for index, row in df.iterrows():
        if row.isna().any():
            nan_errors(row, index)
    df = df.dropna()

    act_session = Actual_session()

    for _, row in df.iterrows():
        try:
            new_row = model(**row.to_dict())
            act_session.add(new_row)
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")

    try:
        act_session.commit()
        print(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        act_session.rollback()
        logging.error(f"Error committing data to {table_name}: {e}")
    finally:
        act_session.close()


def main():
    """
    Main function to load data from CSV files into the database with expected columns.
    """
    create_tables()
    print("Initializing loading data process from CSV files into the database...")
    load_data('data/departments.csv', Department, 'deparments', ['id', 'department'])
    load_data('data/jobs.csv', Job, 'jobs', ['id', 'job'])
    load_data('data/hired_employees.csv', HiredEmployee, 'hired_employees', ['id', 'name', 'datetime', 'department_id', 'job_id'])

    print("Data loaded successfully. Check the error_data.log file for any errors.")

if __name__ == "__main__":
    main()