"""Dimension tables for star schema - Pydantic models with validation"""

from datetime import datetime, date
from typing import Optional
from decimal import Decimal
from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


class DimCliente(BaseModel):
    """Cliente (Client) Dimension Table"""
    
    cliente_key: int = Field(..., description="Auto-increment surrogate key")
    cliente_id_natural: int = Field(..., description="Natural key from source system")
    nombre: str = Field(..., min_length=1, max_length=100, description="Client first name")
    apellido: str = Field(..., min_length=1, max_length=100, description="Client last name")
    email: EmailStr = Field(..., max_length=150, description="Unique email address")
    telefono: Optional[str] = Field(None, max_length=20, description="Phone number")
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Registration date")

    @field_validator('nombre', 'apellido', mode='before')
    @classmethod
    def strip_whitespace(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "cliente_id_natural": 1,
                "nombre": "Juan",
                "apellido": "Pérez",
                "email": "juan.perez@email.com",
                "telefono": "5551234567",
                "fecha_registro": "2026-04-05T10:00:00"
            }
        }


class DimProducto(BaseModel):
    """Producto (Product) Dimension Table"""
    
    producto_key: int = Field(..., description="Auto-increment surrogate key")
    producto_id_natural: int = Field(..., description="Natural key from source system")
    sku: str = Field(..., min_length=1, max_length=50, description="Stock Keeping Unit")
    nombre: str = Field(..., min_length=1, max_length=150, description="Product name")
    descripcion: Optional[str] = Field(None, description="Product description")
    categoria: str = Field(..., min_length=1, max_length=100, description="Product category")
    precio_actual: Decimal = Field(..., decimal_places=2, ge=0, description="Current price")
    activo: bool = Field(default=True, description="Whether product is active")
    fecha_creacion: datetime = Field(default_factory=datetime.now, description="Creation date")

    @field_validator('sku', mode='before')
    @classmethod
    def validate_sku(cls, v):
        if isinstance(v, str):
            return v.strip().upper()
        return v

    @field_validator('precio_actual', mode='before')
    @classmethod
    def validate_precio(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "producto_id_natural": 1,
                "sku": "SKU001",
                "nombre": "Laptop",
                "descripcion": "Laptop Dell 15 pulgadas",
                "categoria": "Electrónica",
                "precio_actual": "899.99",
                "activo": True,
                "fecha_creacion": "2026-04-05T10:00:00"
            }
        }


class DimSucursal(BaseModel):
    """Sucursal (Branch) Dimension Table"""
    
    sucursal_key: int = Field(..., description="Auto-increment surrogate key")
    sucursal_id_natural: int = Field(..., description="Natural key from source system")
    codigo_sucursal: str = Field(..., min_length=1, max_length=30, description="Branch code")
    nombre: str = Field(..., min_length=1, max_length=120, description="Branch name")
    calle: str = Field(..., min_length=1, max_length=150, description="Street address")
    numero: Optional[str] = Field(None, max_length=20, description="Street number")
    colonia: Optional[str] = Field(None, max_length=100, description="Neighborhood/district")
    ciudad: str = Field(..., min_length=1, max_length=100, description="City")
    estado: str = Field(..., min_length=1, max_length=100, description="State/Province")
    pais: str = Field(default="México", max_length=100, description="Country")
    codigo_postal: Optional[str] = Field(None, max_length=10, description="Postal code")
    latitud: Optional[Decimal] = Field(None, decimal_places=6, description="Latitude coordinate")
    longitud: Optional[Decimal] = Field(None, decimal_places=6, description="Longitude coordinate")

    @field_validator('latitud', 'longitud', mode='before')
    @classmethod
    def validate_coordinates(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

    @field_validator('latitud', mode='after')
    @classmethod
    def validate_lat_range(cls, v):
        if v is not None and (v < -90 or v > 90):
            raise ValueError("Latitude must be between -90 and 90")
        return v

    @field_validator('longitud', mode='after')
    @classmethod
    def validate_lon_range(cls, v):
        if v is not None and (v < -180 or v > 180):
            raise ValueError("Longitude must be between -180 and 180")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "sucursal_id_natural": 1,
                "codigo_sucursal": "SC001",
                "nombre": "Sucursal Centro",
                "calle": "Avenida Paseo de la Reforma",
                "numero": "505",
                "colonia": "Cuauhtémoc",
                "ciudad": "Ciudad de México",
                "estado": "México",
                "pais": "México",
                "codigo_postal": "06500",
                "latitud": "19.432600",
                "longitud": "-99.133200"
            }
        }


class DimFecha(BaseModel):
    """Fecha (Date) Dimension Table"""
    
    fecha_key: int = Field(..., description="Date key in YYYYMMDD format")
    fecha: date = Field(..., description="Actual date")
    anio: int = Field(..., ge=1900, le=2100, description="Year")
    trimestre: int = Field(..., ge=1, le=4, description="Quarter (1-4)")
    mes: int = Field(..., ge=1, le=12, description="Month (1-12)")
    nombre_mes: str = Field(..., description="Month name in Spanish")
    semana_anio: int = Field(..., ge=1, le=53, description="Week of year")
    dia: int = Field(..., ge=1, le=31, description="Day of month")
    nombre_dia: str = Field(..., description="Day name in Spanish")
    es_fin_de_semana: bool = Field(..., description="Whether day is weekend")

    @field_validator('fecha_key', mode='before')
    @classmethod
    def validate_fecha_key(cls, v):
        """Validate that fecha_key follows YYYYMMDD format"""
        if isinstance(v, str):
            v = int(v)
        if not (10000000 <= v <= 99999999):
            raise ValueError("fecha_key must be in YYYYMMDD format (8 digits)")
        return v

    @classmethod
    def from_date(cls, date_obj: date) -> 'DimFecha':
        """Factory method to create DimFecha from a date object"""
        meses = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                 "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        
        fecha_key = int(date_obj.strftime("%Y%m%d"))
        anio = date_obj.year
        trimestre = (date_obj.month - 1) // 3 + 1
        mes = date_obj.month
        nombre_mes = meses[mes]
        semana_anio = date_obj.isocalendar()[1]
        dia = date_obj.day
        nombre_dia = dias[date_obj.weekday()]
        es_fin_de_semana = date_obj.weekday() >= 5  # Saturday=5, Sunday=6
        
        return cls(
            fecha_key=fecha_key,
            fecha=date_obj,
            anio=anio,
            trimestre=trimestre,
            mes=mes,
            nombre_mes=nombre_mes,
            semana_anio=semana_anio,
            dia=dia,
            nombre_dia=nombre_dia,
            es_fin_de_semana=es_fin_de_semana
        )

    class Config:
        json_schema_extra = {
            "example": {
                "fecha_key": 20260405,
                "fecha": "2026-04-05",
                "anio": 2026,
                "trimestre": 2,
                "mes": 4,
                "nombre_mes": "Abril",
                "semana_anio": 14,
                "dia": 5,
                "nombre_dia": "Domingo",
                "es_fin_de_semana": True
            }
        }


class DimEstadoOrden(BaseModel):
    """Estado Orden (Order Status) Dimension Table"""
    
    estado_key: int = Field(..., description="Auto-increment primary key")
    estado: str = Field(..., min_length=1, max_length=50, description="Order status")

    @field_validator('estado', mode='before')
    @classmethod
    def validate_estado(cls, v):
        allowed_estados = {"pendiente", "pagada", "enviada", "entregada", "cancelada"}
        if isinstance(v, str):
            v = v.strip().lower()
            if v not in allowed_estados:
                raise ValueError(f"Estado must be one of: {', '.join(allowed_estados)}")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "estado": "pagada"
            }
        }


class DimMetodoPago(BaseModel):
    """Metodo Pago (Payment Method) Dimension Table"""
    
    metodo_pago_key: int = Field(..., description="Auto-increment primary key")
    metodo_pago: str = Field(..., min_length=1, max_length=50, description="Payment method")

    @field_validator('metodo_pago', mode='before')
    @classmethod
    def validate_metodo(cls, v):
        allowed_metodos = {"tarjeta_credito", "tarjeta_debito", "transferencia", "efectivo", "paypal"}
        if isinstance(v, str):
            v = v.strip().lower()
            if v not in allowed_metodos:
                raise ValueError(f"Metodo pago must be one of: {', '.join(allowed_metodos)}")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "metodo_pago": "tarjeta_credito"
            }
        }


class FactVentas(BaseModel):
    """Ventas (Sales) Fact Table"""
    
    venta_key: int = Field(..., description="Auto-increment fact key")
    
    # Degenerate dimensions
    orden_id: int = Field(..., description="Order ID (degenerate dimension)")
    detalle_orden_id: int = Field(..., description="Order detail ID (degenerate dimension)")
    
    # Foreign keys to dimensions
    cliente_key: int = Field(..., description="Foreign key to DimCliente")
    producto_key: int = Field(..., description="Foreign key to DimProducto")
    sucursal_key: int = Field(..., description="Foreign key to DimSucursal")
    fecha_key: int = Field(..., description="Foreign key to DimFecha")
    estado_key: int = Field(..., description="Foreign key to DimEstadoOrden")
    metodo_pago_key: int = Field(..., description="Foreign key to DimMetodoPago")
    
    # Measures
    cantidad: int = Field(..., ge=1, description="Quantity sold")
    precio_unitario: Decimal = Field(..., decimal_places=2, ge=0, description="Unit price")
    subtotal: Decimal = Field(..., decimal_places=2, ge=0, description="Line item total")

    @field_validator('precio_unitario', 'subtotal', mode='before')
    @classmethod
    def validate_decimals(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

    @model_validator(mode='after')
    def validate_subtotal(self):
        """Verify subtotal = cantidad * precio_unitario"""
        calculated = self.cantidad * self.precio_unitario
        if abs(self.subtotal - calculated) > Decimal('0.01'):
            raise ValueError(
                f"Subtotal {self.subtotal} does not match cantidad * precio_unitario = {calculated}"
            )
        return self

    class Config:
        json_schema_extra = {
            "example": {
                "orden_id": 1,
                "detalle_orden_id": 1,
                "cliente_key": 1,
                "producto_key": 1,
                "sucursal_key": 1,
                "fecha_key": 20260405,
                "estado_key": 1,
                "metodo_pago_key": 1,
                "cantidad": 2,
                "precio_unitario": "899.99",
                "subtotal": "1799.98"
            }
        }
