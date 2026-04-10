"""
Entidades (Entities) package - All data models
"""

# Star Schema Dimension Tables
from .dimensiones import (
    DimCliente,
    DimProducto,
    DimSucursal,
    DimFecha,
    DimEstadoOrden,
    DimMetodoPago,
    FactVentas
)

# Original Transactional Models
from .cliente import Cliente
from .sucursal import Sucursal
from .orden_compra import OrdenCompra
from .detalle_orden import DetalleOrden

__all__ = [
    # Star Schema Dimensions
    'DimCliente',
    'DimProducto',
    'DimSucursal',
    'DimFecha',
    'DimEstadoOrden',
    'DimMetodoPago',
    'FactVentas',
    
    # Transactional Models
    'Cliente',
    'Sucursal',
    'OrdenCompra',
    'DetalleOrden',
]
