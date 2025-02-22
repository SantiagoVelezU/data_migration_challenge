from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from .database import Actual_session, engine
from .models import Base, Department, Job, HiredEmployee
from .schemas import DepartmentSchema, JobSchema, HiredEmployeeSchema
from .query import get_department, get_departments, insert_department, get_job, get_jobs, insert_job, get_employee, insert_employee
from typing import List as lista

app = FastAPI()

app.title="Data Challenge API"
app.description="This is an API for data challenge project.You can perform CRUD operations on tables"
app.version="1.0"


# Dependency to get db session
def get_db():
    db = Actual_session()
    try:
        yield db
    finally:
        db.close()


# Deparments endpoints
@app.get("/departments/", response_model=lista[DepartmentSchema])
def read_departments(db: Session = Depends(get_db)):
    return get_departments(db)

@app.get("/departments/{department_id}", response_model=DepartmentSchema)
def read_department(department_id: int, db: Session = Depends(get_db)):
    department = get_department(db, department_id)
    if department is None:
        raise HTTPException(status_code=404, detail="Department not found")
    return department

@app.post("/departments/", response_model=DepartmentSchema)
def create_deparment(department: DepartmentSchema, db: Session = Depends(get_db)):
    return insert_department(db, department)


#Jobs endpoints
@app.get("/jobs/", response_model=lista[JobSchema])
def read_jobs(db: Session =Depends(get_db)):
    return get_jobs(db)

@app.get("/jobs/{job_id}", response_model=JobSchema)
def read_job(job_id:int, db: Session = Depends(get_db)):
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/jobs/", response_model=JobSchema)
def cretae_job(job:JobSchema, db: Session = Depends(get_db)):
    return insert_job(db, job)

# Employees endpoints
@app.get("/employees/{employee_id}", response_model=HiredEmployeeSchema)
def read_job(employee_id:int, db: Session = Depends(get_db)):
    employee = get_employee(db, employee_id)
    if employee is None:
        raise HTTPException(status_code=404, detail="Department not found")
    return employee

@app.post("/employees/", response_model=HiredEmployeeSchema)
def cretae_employee(employee:HiredEmployeeSchema, db: Session = Depends(get_db)):
    return insert_employee(db, employee)