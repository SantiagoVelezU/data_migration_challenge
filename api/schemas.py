# Contains the Pydantic models that will be used to validate the data received by the API endpoints. 
# These models define the structure of the data that the API expects and returns. 
from pydantic import BaseModel, Field
from datetime import datetime as DateTime

class DepartmentSchema(BaseModel):
    id: int = Field(..., ge=1, description="Department's unique identifier.")
    department: str = Field(..., description="Department's name.")

    class Config:
        from_attributes = True

class JobSchema(BaseModel):
    id: int = Field(..., ge=1, description="Job's unique identifier.")
    job: str = Field(..., description="Job's name.")

    class Config:
        from_attributes = True

class HiredEmployeeSchema(BaseModel):
    id: int = Field(..., ge=1, description="Employee's unique identifier.")
    first_name: str = Field(..., description="Employee's first name.")
    datetime: DateTime = Field(..., description="Date and time of hiring.")
    department_id: int = Field(..., ge=1, description="Department's unique identifier.")
    job_id: int = Field(..., ge=1, description="Job's unique identifier.")

    class Config:
        from_attributes = True