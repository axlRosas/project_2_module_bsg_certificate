"""
Database interaction utilities for both transactional and star schema databases.
Provides helper functions for querying and analyzing data.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, Dict, List, Tuple


DB_DIR = Path(__file__).parent / "db"
DB_TRANSACTIONAL = DB_DIR / "transactional.sqlite"
DB_STAR_SCHEMA = DB_DIR / "star_schema.sqlite"


def get_connection(db_type: str = "transactional") -> sqlite3.Connection:
    """
    Get a database connection.
    
    Args:
        db_type: "transactional" or "star_schema"
        
    Returns:
        sqlite3.Connection object
        
    Raises:
        ValueError: If invalid db_type
        FileNotFoundError: If database file not found
    """
    if db_type == "transactional":
        db_path = DB_TRANSACTIONAL
    elif db_type == "star_schema":
        db_path = DB_STAR_SCHEMA
    else:
        raise ValueError(f"Invalid db_type: {db_type}. Use 'transactional' or 'star_schema'")
    
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    return sqlite3.connect(db_path)


def get_table_as_dataframe(table_name: str, db_type: str = "star_schema", 
                           limit: Optional[int] = None) -> pd.DataFrame:
    """
    Retrieve a table as a pandas DataFrame.
    
    Args:
        table_name: Name of the table to retrieve
        db_type: "transactional" or "star_schema"
        limit: Maximum number of rows to retrieve (None for all)
        
    Returns:
        pandas DataFrame
    """
    conn = get_connection(db_type)
    try:
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_star_schema_summary() -> Dict[str, int]:
    """
    Get record counts for all star schema tables.
    
    Returns:
        Dictionary with table names and record counts
    """
    conn = get_connection("star_schema")
    summary = {}
    
    tables = [
        "DimCliente", "DimProducto", "DimSucursal", 
        "DimFecha", "DimEstadoOrden", "DimMetodoPago", "FactVentas"
    ]
    
    try:
        for table in tables:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
                summary[table] = count
            except sqlite3.OperationalError:
                summary[table] = 0
    finally:
        conn.close()
    
    return summary


def get_sales_fact_with_dimensions(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Retrieve FactVentas joined with all relevant dimensions.
    
    Args:
        limit: Maximum number of rows to retrieve
        
    Returns:
        pandas DataFrame with denormalized sales data
    """
    conn = get_connection("star_schema")
    
    query = """
    SELECT 
        fv.venta_key,
        fv.orden_id,
        fv.detalle_orden_id,
        dc.nombre AS cliente_nombre,
        dc.apellido AS cliente_apellido,
        dc.email AS cliente_email,
        dp.sku AS producto_sku,
        dp.nombre AS producto_nombre,
        dp.categoria AS categoria,
        ds.nombre AS sucursal_nombre,
        ds.ciudad AS ciudad,
        df.fecha,
        df.nombre_mes AS mes,
        df.anio,
        de.estado AS estado_orden,
        dm.metodo_pago,
        fv.cantidad,
        fv.precio_unitario,
        fv.subtotal
    FROM FactVentas fv
    JOIN DimCliente dc ON fv.cliente_key = dc.cliente_key
    JOIN DimProducto dp ON fv.producto_key = dp.producto_key
    JOIN DimSucursal ds ON fv.sucursal_key = ds.sucursal_key
    JOIN DimFecha df ON fv.fecha_key = df.fecha_key
    JOIN DimEstadoOrden de ON fv.estado_key = de.estado_key
    JOIN DimMetodoPago dm ON fv.metodo_pago_key = dm.metodo_pago_key
    ORDER BY df.fecha DESC, fv.venta_key DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_sales_by_cliente(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Get total sales amount grouped by cliente.
    
    Args:
        limit: Maximum number of clients to retrieve
        
    Returns:
        pandas DataFrame with cliente info and sales totals
    """
    conn = get_connection("star_schema")
    
    query = """
    SELECT 
        dc.cliente_key,
        dc.nombre,
        dc.apellido,
        dc.email,
        COUNT(DISTINCT fv.orden_id) AS numero_ordenes,
        SUM(fv.subtotal) AS total_ventas,
        AVG(fv.subtotal) AS promedio_venta,
        COUNT(*) AS numero_items
    FROM FactVentas fv
    JOIN DimCliente dc ON fv.cliente_key = dc.cliente_key
    GROUP BY dc.cliente_key, dc.nombre, dc.apellido, dc.email
    ORDER BY total_ventas DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_sales_by_producto(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Get sales metrics grouped by producto.
    
    Args:
        limit: Maximum number of products to retrieve
        
    Returns:
        pandas DataFrame with product sales metrics
    """
    conn = get_connection("star_schema")
    
    query = """
    SELECT 
        dp.producto_key,
        dp.sku,
        dp.nombre,
        dp.categoria,
        dp.precio_actual,
        COUNT(*) AS cantidad_vendida,
        SUM(fv.subtotal) AS total_ventas,
        AVG(fv.precio_unitario) AS precio_promedio,
        MIN(fv.precio_unitario) AS precio_minimo,
        MAX(fv.precio_unitario) AS precio_maximo
    FROM FactVentas fv
    JOIN DimProducto dp ON fv.producto_key = dp.producto_key
    GROUP BY dp.producto_key, dp.sku, dp.nombre, dp.categoria, dp.precio_actual
    ORDER BY total_ventas DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_sales_by_sucursal(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Get sales metrics grouped by sucursal.
    
    Args:
        limit: Maximum number of branches to retrieve
        
    Returns:
        pandas DataFrame with branch sales metrics
    """
    conn = get_connection("star_schema")
    
    query = """
    SELECT 
        ds.sucursal_key,
        ds.codigo_sucursal,
        ds.nombre,
        ds.ciudad,
        ds.estado,
        COUNT(DISTINCT fv.orden_id) AS numero_ordenes,
        COUNT(*) AS numero_items,
        SUM(fv.subtotal) AS total_ventas,
        AVG(fv.subtotal) AS promedio_venta
    FROM FactVentas fv
    JOIN DimSucursal ds ON fv.sucursal_key = ds.sucursal_key
    GROUP BY ds.sucursal_key, ds.codigo_sucursal, ds.nombre, ds.ciudad, ds.estado
    ORDER BY total_ventas DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_sales_by_fecha_range(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get sales within a date range.
    
    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        
    Returns:
        pandas DataFrame with sales data for the date range
    """
    conn = get_connection("star_schema")
    
    query = f"""
    SELECT 
        fv.venta_key,
        df.fecha,
        df.nombre_mes,
        dc.nombre || ' ' || dc.apellido AS cliente,
        dp.nombre AS producto,
        fv.cantidad,
        fv.precio_unitario,
        fv.subtotal,
        de.estado,
        dm.metodo_pago
    FROM FactVentas fv
    JOIN DimFecha df ON fv.fecha_key = df.fecha_key
    JOIN DimCliente dc ON fv.cliente_key = dc.cliente_key
    JOIN DimProducto dp ON fv.producto_key = dp.producto_key
    JOIN DimEstadoOrden de ON fv.estado_key = de.estado_key
    JOIN DimMetodoPago dm ON fv.metodo_pago_key = dm.metodo_pago_key
    WHERE df.fecha BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY df.fecha ASC
    """
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_sales_by_status(status: str) -> pd.DataFrame:
    """
    Get sales filtered by order status.
    
    Args:
        status: Order status (e.g., 'pagada', 'pendiente', 'entregada')
        
    Returns:
        pandas DataFrame with sales for specific status
    """
    conn = get_connection("star_schema")
    
    query = f"""
    SELECT 
        fv.venta_key,
        df.fecha,
        dc.nombre || ' ' || dc.apellido AS cliente,
        dp.nombre AS producto,
        fv.cantidad,
        fv.subtotal,
        dm.metodo_pago
    FROM FactVentas fv
    JOIN DimFecha df ON fv.fecha_key = df.fecha_key
    JOIN DimCliente dc ON fv.cliente_key = dc.cliente_key
    JOIN DimProducto dp ON fv.producto_key = dp.producto_key
    JOIN DimEstadoOrden de ON fv.estado_key = de.estado_key
    JOIN DimMetodoPago dm ON fv.metodo_pago_key = dm.metodo_pago_key
    WHERE de.estado = '{status.lower()}'
    ORDER BY df.fecha DESC
    """
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_revenue_by_month() -> pd.DataFrame:
    """
    Get total revenue grouped by month.
    
    Returns:
        pandas DataFrame with monthly revenue
    """
    conn = get_connection("star_schema")
    
    query = """
    SELECT 
        df.anio,
        df.mes,
        df.nombre_mes,
        COUNT(DISTINCT fv.orden_id) AS numero_ordenes,
        COUNT(*) AS numero_items,
        SUM(fv.subtotal) AS total_ingresos,
        AVG(fv.subtotal) AS promedio_venta
    FROM FactVentas fv
    JOIN DimFecha df ON fv.fecha_key = df.fecha_key
    GROUP BY df.anio, df.mes, df.nombre_mes
    ORDER BY df.anio ASC, df.mes ASC
    """
    
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def validate_data_quality() -> Dict[str, any]:
    """
    Perform basic data quality checks on the star schema.
    
    Returns:
        Dictionary with validation results
    """
    conn = get_connection("star_schema")
    issues = []
    
    try:
        # Check for null values in FactVentas foreign keys
        query = """
        SELECT 
            SUM(CASE WHEN cliente_key IS NULL OR cliente_key = 0 THEN 1 ELSE 0 END) as null_clientes,
            SUM(CASE WHEN producto_key IS NULL OR producto_key = 0 THEN 1 ELSE 0 END) as null_productos,
            SUM(CASE WHEN sucursal_key IS NULL OR sucursal_key = 0 THEN 1 ELSE 0 END) as null_sucursales,
            SUM(CASE WHEN fecha_key IS NULL THEN 1 ELSE 0 END) as null_fechas,
            SUM(CASE WHEN estado_key IS NULL THEN 1 ELSE 0 END) as null_estados,
            SUM(CASE WHEN metodo_pago_key IS NULL THEN 1 ELSE 0 END) as null_metodos
        FROM FactVentas
        """
        
        result = pd.read_sql(query, conn).iloc[0]
        
        # Check for negative amounts
        negatives = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM FactVentas WHERE cantidad < 0 OR precio_unitario < 0 OR subtotal < 0",
            conn
        )['cnt'][0]
        
        # Check for subtotal mismatches
        mismatches = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM FactVentas WHERE ABS(subtotal - (cantidad * precio_unitario)) > 0.01",
            conn
        )['cnt'][0]
        
        validation = {
            'null_clientes': int(result['null_clientes']),
            'null_productos': int(result['null_productos']),
            'null_sucursales': int(result['null_sucursales']),
            'null_fechas': int(result['null_fechas']),
            'null_estados': int(result['null_estados']),
            'null_metodos': int(result['null_metodos']),
            'negative_amounts': negatives,
            'subtotal_mismatches': mismatches,
            'is_valid': all([
                result['null_clientes'] == 0,
                result['null_productos'] == 0,
                result['null_sucursales'] == 0,
                result['null_fechas'] == 0,
                result['null_estados'] == 0,
                result['null_metodos'] == 0,
                negatives == 0,
                mismatches == 0
            ])
        }
        
        return validation
    
    finally:
        conn.close()


if __name__ == "__main__":
    # Example usage
    print("Star Schema Summary:")
    summary = get_star_schema_summary()
    for table, count in summary.items():
        print(f"  {table}: {count:,}")
    
    print("\nSales by Cliente:")
    df_by_cliente = get_sales_by_cliente(limit=5)
    print(df_by_cliente)
    
    print("\nSales by Producto:")
    df_by_producto = get_sales_by_producto(limit=5)
    print(df_by_producto)
    
    print("\nData Quality Validation:")
    validation = validate_data_quality()
    print(f"  Valid: {validation['is_valid']}")
    for key, value in validation.items():
        if key != 'is_valid':
            print(f"  {key}: {value}")
