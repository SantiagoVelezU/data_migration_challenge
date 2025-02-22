#This file contains the functions that will be used to query the database. 
# The functions will be used in the API to interact with the database (Including challenge#2).
from sqlalchemy.orm import Session
from .schemas import DepartmentSchema, JobSchema, HiredEmployeeSchema
from .models import Department, Job, HiredEmployee

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
def insert_employee(db: Session, employee: HiredEmployeeSchema):
    db_employee = HiredEmployee(id=employee.id, 
                                first_name=employee.first_name, 
                                datetime=employee.datetime, 
                                department_id=employee.department_id, 
                                job_id=employee.job_id)
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee

#This function will get an employee by its id from the hired_employees table in the database.
def get_employee(db: Session, employee_id: int):
    return db.query(HiredEmployee).filter(HiredEmployee.id == employee_id).first()
