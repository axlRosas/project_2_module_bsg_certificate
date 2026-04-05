"""Cliente entity with pydantic validations"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field, field_validator


class Cliente(BaseModel):
    """Represents a customer in the system"""
    
    id_cliente: int = Field(..., description="Auto-increment primary key")
    name: str = Field(..., min_length=1, max_length=100, description="Customer first name")
    last_name: str = Field(..., min_length=1, max_length=100, description="Customer last name")
    email: EmailStr = Field(..., max_length=150, description="Unique email address")
    phone: Optional[str] = Field(None, max_length=20, description="Phone number")
    registration_date: datetime = Field(default_factory=datetime.now, description="Registration date")

    @field_validator('name', 'last_name', mode='before')
    @classmethod
    def strip_whitespace(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    def display_info(self):
        """Display customer information"""
        print(f"ID: {self.id_cliente}")
        print(f"Name: {self.name} {self.last_name}")
        print(f"Email: {self.email}")
        print(f"Phone: {self.phone}")
        print(f"Registration Date: {self.registration_date}")