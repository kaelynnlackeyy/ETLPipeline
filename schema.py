from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, field_validator

class covid_schema(BaseModel):
   state_code: str = Field(..., min_length=2, max_length=2)
   state_name: str
   date: date
   cases_total: Optional[int] = Field(default=None, ge=0)
   cases_confirmed: Optional[int] = Field(default=None, ge=0)
   deaths_total: Optional[int] = Field(default=None, ge=0)
   deaths_confirmed: Optional[int] = Field(default=None, ge=0)
   deaths_probable: Optional[int] = Field(default=None, ge=0)
   hospitalized_currently: Optional[int] = Field(default=None, ge=0)
   hospitalized_cumulative: Optional[int] = Field(default=None, ge=0)
   in_icu_currently: Optional[int] = Field(default=None, ge=0)
   tests_total: Optional[int] = Field(default=None, ge=0)
   @field_validator('state_code', mode='before')
   
   def uppercase_state(cls, v):
        return v.upper() if v else v