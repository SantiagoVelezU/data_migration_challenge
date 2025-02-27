from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base

class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True, index=True, autoincrement=False)
    department = Column(String, nullable=False)
    employees = relationship("HiredEmployee", back_populates="department")

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True, autoincrement=False)
    job = Column(String, nullable=False)
    employees = relationship('HiredEmployee', back_populates='job')

class HiredEmployee(Base):
    __tablename__ = "hired_employees"
    id = Column(Integer, primary_key=True, index=True, autoincrement=False)
    name = Column(String, nullable=False)
    datetime = Column(DateTime, nullable=False)
    department_id = Column(Integer, ForeignKey("departments.id"), nullable=False)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    department = relationship('Department', back_populates='employees')
    job = relationship('Job', back_populates='employees')