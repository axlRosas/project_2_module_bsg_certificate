"""DetalleOrden (Order Details) entity with pydantic validations"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal


class DetalleOrden(BaseModel):
    """Represents the details of items in a purchase order"""
    
    id: Optional[int] = Field(None, description="Auto-increment primary key")
    orden_id: int = Field(..., gt=0, description="Order ID (foreign key)")
    producto_id: int = Field(..., gt=0, description="Product ID (foreign key)")
    cantidad: int = Field(..., gt=0, description="Quantity of items")
    precio_unitario: Decimal = Field(..., decimal_places=2, max_digits=10, gt=0, description="Unit price")
    subtotal: Decimal = Field(..., decimal_places=2, max_digits=12, gt=0, description="Line item subtotal")

    @field_validator('precio_unitario', 'subtotal', mode='before')
    @classmethod
    def convert_to_decimal(cls, v):
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        return v

    @field_validator('subtotal')
    @classmethod
    def validate_subtotal(cls, v, info):
        """Validate that subtotal matches cantidad * precio_unitario"""
        if 'cantidad' in info.data and 'precio_unitario' in info.data:
            expected = Decimal(str(info.data['cantidad'])) * info.data['precio_unitario']
            # Allow small rounding differences
            if abs(v - expected) > Decimal('0.01'):
                raise ValueError(
                    f"Subtotal {v} does not match cantidad * precio_unitario ({expected})"
                )
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "orden_id": 1,
                "producto_id": 1,
                "cantidad": 5,
                "precio_unitario": "100.00",
                "subtotal": "500.00"
            }
        }
    }
