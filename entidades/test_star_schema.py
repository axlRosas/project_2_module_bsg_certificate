"""
Test and demonstration script for star schema Pydantic models
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from datetime import date, datetime
from decimal import Decimal
from dimensiones import (
    DimCliente,
    DimProducto,
    DimSucursal,
    DimFecha,
    DimEstadoOrden,
    DimMetodoPago,
    FactVentas
)


def test_dim_cliente():
    """Test DimCliente model"""
    print("\n" + "="*60)
    print("Testing DimCliente")
    print("="*60)
    
    cliente = DimCliente(
        cliente_key=1,
        cliente_id_natural=1,
        nombre="Juan",
        apellido="Pérez",
        email="juan.perez@email.com",
        telefono="5551234567"
    )
    print(f"✓ Cliente created: {cliente.nombre} {cliente.apellido}")
    print(f"  Email: {cliente.email}")
    print(f"  Pydantic model valid: {cliente.model_validate(cliente.model_dump())}")


def test_dim_producto():
    """Test DimProducto model"""
    print("\n" + "="*60)
    print("Testing DimProducto")
    print("="*60)
    
    producto = DimProducto(
        producto_key=1,
        producto_id_natural=1,
        sku="sku001",
        nombre="Laptop",
        descripcion="Laptop Dell 15 pulgadas",
        categoria="Electrónica",
        precio_actual=Decimal("899.99")
    )
    print(f"✓ Producto created: {producto.nombre}")
    print(f"  SKU: {producto.sku}")
    print(f"  Precio: ${producto.precio_actual}")
    print(f"  Activo: {producto.activo}")


def test_dim_sucursal():
    """Test DimSucursal model"""
    print("\n" + "="*60)
    print("Testing DimSucursal")
    print("="*60)
    
    sucursal = DimSucursal(
        sucursal_key=1,
        sucursal_id_natural=1,
        codigo_sucursal="SC001",
        nombre="Sucursal Centro",
        calle="Avenida Paseo de la Reforma",
        numero="505",
        colonia="Cuauhtémoc",
        ciudad="Ciudad de México",
        estado="México",
        pais="México",
        codigo_postal="06500",
        latitud=Decimal("19.432600"),
        longitud=Decimal("-99.133200")
    )
    print(f"✓ Sucursal created: {sucursal.nombre}")
    print(f"  Código: {sucursal.codigo_sucursal}")
    print(f"  Ubicación: {sucursal.calle} #{sucursal.numero}, {sucursal.ciudad}")
    print(f"  Coordenadas: ({sucursal.latitud}, {sucursal.longitud})")


def test_dim_fecha():
    """Test DimFecha model"""
    print("\n" + "="*60)
    print("Testing DimFecha")
    print("="*60)
    
    # Test with factory method
    fecha_obj = date(2026, 4, 5)
    dim_fecha = DimFecha.from_date(fecha_obj)
    print(f"✓ DimFecha created from date: {fecha_obj}")
    print(f"  Fecha Key: {dim_fecha.fecha_key}")
    print(f"  Año: {dim_fecha.anio}")
    print(f"  Trimestre: {dim_fecha.trimestre}")
    print(f"  Mes: {dim_fecha.mes} ({dim_fecha.nombre_mes})")
    print(f"  Día: {dim_fecha.nombre_dia}")
    print(f"  Es fin de semana: {dim_fecha.es_fin_de_semana}")
    print(f"  Semana del año: {dim_fecha.semana_anio}")


def test_dim_estado_orden():
    """Test DimEstadoOrden model"""
    print("\n" + "="*60)
    print("Testing DimEstadoOrden")
    print("="*60)
    
    estado = DimEstadoOrden(
        estado_key=1,
        estado="PAGADA")  # Test case normalization
    print(f"✓ EstadoOrden created: {estado.estado}")
    
    # Test validation error
    try:
        bad_estado = DimEstadoOrden(
            estado_key=2,
            estado="invalido")
    except ValueError as e:
        print(f"✓ Validation error caught (expected): {e}")


def test_dim_metodo_pago():
    """Test DimMetodoPago model"""
    print("\n" + "="*60)
    print("Testing DimMetodoPago")
    print("="*60)
    
    metodo = DimMetodoPago(
        metodo_pago_key=1,
        metodo_pago="TARJETA_CREDITO")  # Test case normalization
    print(f"✓ MetodoPago created: {metodo.metodo_pago}")
    
    # Test validation error
    try:
        bad_metodo = DimMetodoPago(
            metodo_pago_key=2,
            metodo_pago="bitcoin")
    except ValueError as e:
        print(f"✓ Validation error caught (expected): {e}")


def test_fact_ventas():
    """Test FactVentas model"""
    print("\n" + "="*60)
    print("Testing FactVentas")
    print("="*60)
    
    venta = FactVentas(
        venta_key=1,
        orden_id=1,
        detalle_orden_id=1,
        cliente_key=1,
        producto_key=1,
        sucursal_key=1,
        fecha_key=20260405,
        estado_key=1,
        metodo_pago_key=1,
        cantidad=2,
        precio_unitario=Decimal("899.99"),
        subtotal=Decimal("1799.98")
    )
    print(f"✓ FactVentas created")
    print(f"  Orden: {venta.orden_id}")
    print(f"  Cantidad: {venta.cantidad}")
    print(f"  Precio Unitario: ${venta.precio_unitario}")
    print(f"  Subtotal: ${venta.subtotal}")
    
    # Test subtotal validation error
    try:
        bad_venta = FactVentas(
            venta_key=2,
            orden_id=1,
            detalle_orden_id=1,
            cliente_key=1,
            producto_key=1,
            sucursal_key=1,
            fecha_key=20260405,
            estado_key=1,
            metodo_pago_key=1,
            cantidad=2,
            precio_unitario=Decimal("100.00"),
            subtotal=Decimal("500.00")  # Wrong: 2 * 100 = 200, not 500
        )
    except ValueError as e:
        print(f"✓ Validation error caught (expected): {e}")


def test_json_serialization():
    """Test JSON serialization"""
    print("\n" + "="*60)
    print("Testing JSON Serialization")
    print("="*60)
    
    cliente = DimCliente(
        cliente_key=100,
        cliente_id_natural=1,
        nombre="María",
        apellido="García",
        email="maria.garcia@email.com",
        telefono="5559876543"
    )
    
    # Serialize to JSON
    json_str = cliente.model_dump_json(indent=2)
    print(f"✓ Cliente serialized to JSON:")
    print(json_str)
    
    # Deserialize from JSON
    json_data = '{"cliente_key": 100, "cliente_id_natural": 2, "nombre": "Carlos", "apellido": "López", "email": "carlos.lopez@email.com", "telefono": "5554567890"}'
    cliente_from_json = DimCliente.model_validate_json(json_data)
    print(f"\n✓ Cliente deserialized from JSON: {cliente_from_json.nombre} {cliente_from_json.apellido}")


def test_schema_export():
    """Test JSON schema export"""
    print("\n" + "="*60)
    print("Testing JSON Schema Export")
    print("="*60)
    
    schema = DimCliente.model_json_schema()
    print(f"✓ DimCliente JSON Schema exported")
    print(f"  Properties: {', '.join(schema['properties'].keys())}")
    print(f"  Required fields: {', '.join(schema.get('required', []))}")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("STAR SCHEMA PYDANTIC MODELS - TEST SUITE")
    print("="*60)
    
    test_dim_cliente()
    test_dim_producto()
    test_dim_sucursal()
    test_dim_fecha()
    test_dim_estado_orden()
    test_dim_metodo_pago()
    test_fact_ventas()
    test_json_serialization()
    test_schema_export()
    
    print("\n" + "="*60)
    print("✓ ALL TESTS COMPLETED SUCCESSFULLY")
    print("="*60 + "\n")
