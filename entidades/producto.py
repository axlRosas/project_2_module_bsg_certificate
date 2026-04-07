"""Module for defining product-related entities and data models"""

from datetime import datetime
from typing import Optional
from enum import Enum
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal


class ProductoTipo(str, Enum):
    """Enum for product categories"""
    ELECTRONICA = "mouse", "teclado", "monitor", "laptop"
    ROPA = "mercancia", "camisa", "pantalon", "zapatos"
    ALIMENTOS = "alimentos"
    LIBROS = "libros"

class Producto(BaseModel):
    """Represents a purchase order in the system"""
    id: int = Field(..., description="Auto-increment primary key")
    sku: str = Field(..., min_length=1, max_length=30, description="Unique stock keeping unit")
    nombre: str = Field(..., min_length=1, max_length=120, description="Product name")
    descripcion: Optional[str] = Field(None, max_length=500, description="Product description")
    categoria: ProductoTipo = Field(..., description="Product category")
    precio: Decimal = Field(..., gt=0, description="Product price") #this constrain prices to be greater than 0
    stock: int = Field(..., ge=0, description="Available stock quantity") #this constrain stock to be greater than or equal to 0
    activo: bool = Field(default=True, description="Indicates if the product is active")
    fecha_creacion: datetime = Field(default_factory=datetime.now, description="Creation date of the product record")
    

    @field_validator('sku', 'nombre', 'descripcion', mode='before')
    @classmethod
    def strip_whitespace(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v
    
    @field_validator('precio', mode='before')
    @classmethod
    def validate_precio(cls, v):
        if isinstance(v, (float, int, Decimal)):
            return Decimal(v)
        raise ValueError("Precio must be a numeric value")
    
    
