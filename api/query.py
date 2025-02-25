#This file contains the functions that will be used to query the database. 
# The functions will be used in the API to interact with the database (Including challenge#2).
from sqlalchemy.orm import Session
from .schemas import DepartmentSchema, JobSchema, HiredEmployeeSchema
from .models import Department, Job, HiredEmployee, Base
from typing import List as lista
from sqlalchemy.sql import text
import os
import fastavro
from sqlalchemy import Integer, DateTime
from datetime import datetime
from sqlalchemy.exc import IntegrityError


#This function will insert a new department into departments table in the database.
def insert_department(db: Session, department: DepartmentSchema):
    db_department = Department(id=department.id, 
                                department=department.department)
    db.add(db_department)
    db.commit()
    db.refresh(db_department)
    return db_department

#This function will get a department by its id from the departments table in the database.
def get_department(db: Session, department_id: int):
    return db.query(Department).filter(Department.id == department_id).first()

#This function will get all departments from the departments table in the database so you could see all the departments.
def get_departments(db: Session):
    return db.query(Department).all()


#This function will insert a new job into jobs table in the database.
def insert_job(db: Session, job: JobSchema):
    db_job = Job(id=job.id, 
                        job=job.job)
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

#This function will get a job by its id from the jobs table in the database.
def get_job(db: Session, job_id: int):
    return db.query(Job).filter(Job.id == job_id).first()

#This function will get all jobs from the jobs table in the database so you could see all the jobs.
def get_jobs(db: Session):
    return db.query(Job).all()



#This function will insert a new employee into hired_employees table in the database.
def insert_employee(db: Session, employees: lista[HiredEmployeeSchema]):
    for employee in employees:
        db_employee = HiredEmployee(
            id=employee.id,
            name=employee.name,
            datetime=employee.datetime,
            department_id=employee.department_id,
            job_id=employee.job_id
        )
        db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee

#This function will get an employee by its id from the hired_employees table in the database.
def get_employee(db: Session, employee_id: int):
    return db.query(HiredEmployee).filter(HiredEmployee.id == employee_id).first()



#Challenge #2
def get_employees_by_quarter(db: Session):
    query = text("""
    SELECT 
        t2.department AS department
        ,t3.job AS job
        ,COUNT(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 1 THEN 1 END) AS Q1
        ,COUNT(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 2 THEN 1 END) AS Q2
        ,COUNT(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 3 THEN 1 END) AS Q3
        ,COUNT(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 4 THEN 1 END) AS Q4
    FROM hired_employees t1
    JOIN departments t2
    ON t1.department_id = t2.id
    JOIN jobs t3 
    ON t1.job_id = t3.id
    WHERE EXTRACT(YEAR FROM t1.datetime) = 2021 
    GROUP BY t2.department, t3.job
    ORDER BY t2.department, t3.job;
    """)
    
    result = db.execute(query).mappings().all()
    return [dict(row) for row in result]

def get_departments_above_avg(db: Session):
    query = text("""
    WITH hired_employees AS (
        SELECT
            t1.id
            ,t1.department
            ,COUNT(t2.id) AS n_employees
            FROM departments t1
            JOIN hired_employees t2
            ON t2.department_id = t1.id
            WHERE EXTRACT(YEAR FROM t2.datetime) = 2021 
            GROUP BY 1, 2
        ),
    average AS (
        SELECT 
            AVG(n_employees) AS mean_employees
        FROM hired_employees
    )
    SELECT
        t1.id
        ,t1.department
        ,t1.n_employees
    FROM hired_employees t1
    JOIN average t2
    ON 1 = 1
    WHERE CAST(n_employees AS FLOAT)> CAST(mean_employees AS FLOAT)
    ORDER BY n_employees DESC;
    """)
    result = db.execute(query).mappings().all()
    return [dict(row) for row in result]


#Backups
path_bp = "backups"

def avro_type(column):
    if isinstance(column.type, Integer):
        return "int"
    elif isinstance(column.type, DateTime):
        return {"type": "long", "logicalType": "timestamp-millis"}
    else:
        return "string"

def backup_avro(table_name: str, db: Session):
    model = Base.metadata.tables.get(table_name)
    data = db.execute(model.select()).fetchall()

    schema = {
        "type": "record",
        "name": table_name,
        "fields": [{"name": col.name, "type": avro_type(col)} for col in model.columns]
    }
    
    file = os.path.join(path_bp, f"{table_name}.avro")
    rows =[dict(row._mapping) for row in data]

    with open(file, "wb") as f:
        fastavro.writer(f, schema, rows)
    
    return file

#estore

def restore_table(table_name: str, db: Session):
    file_path = os.path.join(path_bp, f"{table_name}.avro")

    try:
        with open(file_path, "rb") as f:
            reader = fastavro.reader(f)
            rows = [dict(row) for row in reader]

        #Special treatment due to datetime col
        if table_name == "hired_employees":
            employees = []
            for row in rows:
                # Cast to datetime
                if isinstance(row["datetime"], str):
                    row["datetime"] = datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S")
                employees.append(HiredEmployee(**row))
            db.add_all(employees)
        elif table_name == "jobs":
            jobs = []
            [jobs.append(Job(**row)) for row in rows]
            db.add_all(jobs)
        elif table_name == "departments":
            departments = []
            [departments.append(Department(**row)) for row in rows]
            db.add_all(departments)

        db.commit()
        return f"Restauración de '{table_name}' completada"
    
    except Exception as e:
        db.rollback()  # Revertir cambios en caso de error
        raise ValueError(f"Error al restaurar la tabla '{table_name}': {str(e)}")