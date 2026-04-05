"""Sucursal (Branch) entity with pydantic validations"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator


class Sucursal(BaseModel):
    """Represents a branch location in the system"""
    
    id: int = Field(..., description="Auto-increment primary key")
    nombre: str = Field(..., min_length=1, max_length=120, description="Branch name")
    codigo_sucursal: str = Field(..., min_length=1, max_length=30, description="Unique branch code")
    calle: str = Field(..., min_length=1, max_length=150, description="Street name")
    numero: Optional[str] = Field(None, max_length=20, description="Address number")
    colonia: Optional[str] = Field(None, max_length=100, description="Neighborhood")
    ciudad: str = Field(..., min_length=1, max_length=100, description="City")
    estado: str = Field(..., min_length=1, max_length=100, description="State/Province")
    pais: str = Field(default="México", max_length=100, description="Country")
    codigo_postal: Optional[str] = Field(None, max_length=10, description="Postal code")
    latitud: Optional[float] = Field(None, ge=-90, le=90, description="Latitude coordinate")
    longitud: Optional[float] = Field(None, ge=-180, le=180, description="Longitude coordinate")

    @field_validator('nombre', 'codigo_sucursal', 'calle', 'ciudad', 'estado', 'pais', mode='before')
    @classmethod
    def strip_whitespace(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator('latitud', 'longitud', mode='before')
    @classmethod
    def validate_coordinates(cls, v):
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                raise ValueError("Coordinates must be numeric")
        return v
