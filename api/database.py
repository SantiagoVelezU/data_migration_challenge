# This file creates the database connection. It also creates the Base class that is used to create the tables.
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import psycopg2
from psycopg2 import sql, errors

# Load environment variables from .env file (you should create this file in the root of your project)
load_dotenv()


# Get the environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSGREST_HOST = os.getenv("POSTGRES_HOST")
POSGREST_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")


#This function creates a database in postgreSQL using db postgres as default db. Then connection is close.
def create_db():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSGREST_HOST,
            port=POSGREST_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_DB,))
        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(POSTGRES_DB)))
            print(f"{POSTGRES_DB} database has been created successfully.")
    except errors.DuplicateDatabase:
        print(f"La base de datos {POSTGRES_DB} ya existe, no se creó.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
create_db()


# DB url
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSGREST_HOST}:{POSGREST_PORT}/{POSTGRES_DB}"

# Create the database connection and session
engine = create_engine(DATABASE_URL)
Actual_session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# Base class for the database models (important to create our tables)
Base = declarative_base()
