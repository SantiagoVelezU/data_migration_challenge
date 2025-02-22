# This file creates the database connection. It also creates the Base class that is used to create the tables.
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Load environment variables from .env file (you should create this file in the root of your project)
load_dotenv()


# Get the environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSGREST_HOST = os.getenv("POSTGRES_HOST")
POSGREST_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")


# DB url
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSGREST_HOST}:{POSGREST_PORT}/{POSTGRES_DB}"

# Create the database connection and session
engine = create_engine(DATABASE_URL)
Actual_session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# Base class for the database models (important to create our tables)
Base = declarative_base()
