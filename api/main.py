from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from .database import Actual_session, engine
from .schemas import DepartmentSchema, JobSchema, HiredEmployeeSchema
from .query import get_department, get_departments, insert_department, get_job, get_jobs, insert_job, get_employee, insert_employee, get_employees_by_quarter, get_departments_above_avg, backup_avro, restore_table
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


@app.get("/")
def root():
    return "Hola Globant"

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


@app.delete("/departments/{department_id}")
def delete_deparment(department_id: int, session_db: Session = Depends(get_db)):
    db_department = get_department(session_db, department_id)
    session_db.delete(db_department)
    session_db.commit()
    return "Deleted successfully"


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

@app.delete("/jobs/{job_id}")
def delete_job(job_id: int, session_db: Session = Depends(get_db)):
    db_job = get_job(session_db, job_id)
    session_db.delete(db_job)
    session_db.commit()
    return "Deleted successfully"


# Employees endpoints
@app.get("/employees/{employee_id}", response_model=HiredEmployeeSchema)
def read_employee(employee_id:int, db: Session = Depends(get_db)):
    employee = get_employee(db, employee_id)
    print(employee)
    if employee is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    return employee

@app.post("/employees/", response_model=HiredEmployeeSchema)
def cretae_employee(employees:lista[HiredEmployeeSchema], db: Session = Depends(get_db)):
    if not (1<= len(employees)<=1000):
        raise HTTPException(status_code=400, detail="Batch size must be between 1 and 1000")
    return insert_employee(db, employees)

@app.delete("/employees/{employee_id}")
def delete_data(employee_id: int, session_db: Session = Depends(get_db)):
    db_employee = get_employee(session_db, employee_id)
    session_db.delete(db_employee)
    session_db.commit()
    return "Deleted successfully"


#Challenge #2 Endpoints
@app.get("/reports/employees/hired-by-quarter")
def hired_employees_by_quarter(session_db: Session = Depends(get_db)):
    return get_employees_by_quarter(session_db)

@app.get("/reports/employees/department-avg")
def departments_avg(session_db: Session = Depends(get_db)):
    return get_departments_above_avg(session_db)


#Backup Endpoint
@app.post("/backup/{table_name}")
def backup_table(table_name: str, session_db: Session = Depends(get_db)):
    try:
        file = backup_avro(table_name, session_db)
        return {"message": "Backup done", "file": file}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
#Restore Endpoint
@app.post("/restore/{table_name}")
def restore_table_avro(table_name: str, session_db: Session = Depends(get_db)):
    try:
        load = restore_table(table_name, session_db)
        return {"message": load}
    except Exception as e:
        print(f"Error: {e}") 
        raise HTTPException(status_code=400, detail=str(e))