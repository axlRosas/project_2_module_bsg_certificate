"""Orden (Purchase Order) entity with pydantic validations"""

from datetime import datetime
from typing import Optional
from enum import Enum
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal


class EstadoOrden(str, Enum):
    """Enum for order status"""
    PENDIENTE = "pendiente"
    PAGADA = "pagada"
    ENVIADA = "enviada"
    ENTREGADA = "entregada"
    CANCELADA = "cancelada"


class OrdenCompra(BaseModel):
    """Represents a purchase order in the system"""
    
    id: int = Field(..., description="Auto-increment primary key")
    cliente_id: int = Field(..., gt=0, description="Customer ID (foreign key)")
    sucursal_id: int = Field(..., gt=0, description="Branch ID (foreign key)")
    fecha_orden: datetime = Field(default_factory=datetime.now, description="Order date")
    estado: EstadoOrden = Field(default=EstadoOrden.PENDIENTE, description="Order status")
    metodo_pago: Optional[str] = Field(None, max_length=50, description="Payment method")
    total: Decimal = Field(..., decimal_places=2, max_digits=12, gt=0, description="Order total amount")

    @field_validator('total', mode='before')
    @classmethod
    def convert_to_decimal(cls, v):
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        return v

    @field_validator('metodo_pago', mode='before')
    @classmethod
    def strip_whitespace(cls, v):
        if isinstance(v, str):
            return v.strip() or None
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "cliente_id": 1,
                "sucursal_id": 1,
                "fecha_orden": "2026-04-03T10:30:00",
                "estado": "pendiente",
                "metodo_pago": "tarjeta_credito",
                "total": "1500.00"
            }
        }
    }
