"""
ETL Pipeline: Transactional Database to Star Schema Database
Extracts data from transactional.sqlite and loads into star_schema.sqlite using pandas.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from typing import Tuple, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionalToStarPipeline:
    """ETL Pipeline to transform transactional data into star schema."""
    
    def __init__(self):
        """Initialize database paths."""
        self.db_dir = Path(__file__).parent / "db"
        self.source_db = self.db_dir / "transactional.sqlite"
        self.target_db = self.db_dir / "star_schema.sqlite"
        
        if not self.source_db.exists():
            raise FileNotFoundError(f"Transactional database not found: {self.source_db}")
        if not self.target_db.exists():
            raise FileNotFoundError(f"Star schema database not found: {self.target_db}")
    
    def get_source_connection(self) -> sqlite3.Connection:
        """Get connection to transactional database."""
        return sqlite3.connect(self.source_db)
    
    def get_target_connection(self) -> sqlite3.Connection:
        """Get connection to star schema database."""
        return sqlite3.connect(self.target_db)
    
    def extract_table(self, conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
        """Extract data from transactional database."""
        logger.info(f"Extracting {table_name}...")
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    
    def clear_star_tables(self) -> None:
        """Clear all tables in star schema database."""
        logger.info("Clearing star schema tables...")
        conn = self.get_target_connection()
        cursor = conn.cursor()
        
        # Tables in dependency order for cleanup
        tables = [
            "FactVentas", 
            "DimFecha", 
            "DimEstadoOrden", 
            "DimMetodoPago",
            "DimCliente", 
            "DimProducto", 
            "DimSucursal"
        ]
        
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table}")
                logger.info(f"  ✓ Cleared {table}")
            except sqlite3.OperationalError as e:
                logger.warning(f"  ⚠ Could not clear {table}: {e}")
        
        conn.commit()
        conn.close()
    
    def transform_dim_cliente(self, df_clientes: pd.DataFrame) -> pd.DataFrame:
        """Transform Cliente data into DimCliente."""
        logger.info("Transforming DimCliente...")
        
        df_dim = df_clientes[['id', 'nombre', 'apellido', 'email', 'telefono', 'fecha_registro']].copy()
        df_dim.rename(columns={"id": "cliente_id_natural"}, inplace=True)
        
        # Add surrogate key
        df_dim.insert(0, "cliente_key", range(1, len(df_dim) + 1))
        
        # Ensure date format
        df_dim["fecha_registro"] = pd.to_datetime(df_dim["fecha_registro"])
        
        logger.info(f"  ✓ Transformed {len(df_dim)} cliente records")
        return df_dim
    
    def transform_dim_producto(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Transform Producto data into DimProducto."""
        logger.info("Transforming DimProducto...")
        
        df_dim = df_productos[['id', 'sku', 'nombre', 'descripcion', 'categoria', 'precio', 'activo', 'fecha_creacion']].copy()
        df_dim.rename(columns={
            "id": "producto_id_natural",
            "precio": "precio_actual"
        }, inplace=True)
        
        # Add surrogate key
        df_dim.insert(0, "producto_key", range(1, len(df_dim) + 1))
        
        # Ensure date format
        df_dim["fecha_creacion"] = pd.to_datetime(df_dim["fecha_creacion"])
        
        # Normalize SKU to uppercase
        df_dim["sku"] = df_dim["sku"].str.upper()
        
        logger.info(f"  ✓ Transformed {len(df_dim)} producto records")
        return df_dim
    
    def transform_dim_sucursal(self, df_sucursales: pd.DataFrame) -> pd.DataFrame:
        """Transform Sucursal data into DimSucursal."""
        logger.info("Transforming DimSucursal...")
        
        df_dim = df_sucursales[['id', 'codigo_sucursal', 'nombre', 'calle', 'numero', 'colonia', 
                                  'ciudad', 'estado', 'pais', 'codigo_postal', 'latitud', 'longitud']].copy()
        df_dim.rename(columns={"id": "sucursal_id_natural"}, inplace=True)
        
        # Add surrogate key
        df_dim.insert(0, "sucursal_key", range(1, len(df_dim) + 1))
        
        # Ensure numeric coordinate types
        df_dim["latitud"] = pd.to_numeric(df_dim["latitud"], errors='coerce')
        df_dim["longitud"] = pd.to_numeric(df_dim["longitud"], errors='coerce')
        
        logger.info(f"  ✓ Transformed {len(df_dim)} sucursal records")
        return df_dim
    
    def create_dim_fecha(self, start_date: date = date(2024, 1, 1), 
                         end_date: date = date(2027, 12, 31)) -> pd.DataFrame:
        """Create DimFecha dimension table covering a date range."""
        logger.info(f"Creating DimFecha ({start_date} to {end_date})...")
        
        meses = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                 "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        records = []
        for idx, dt in enumerate(dates, 1):
            trimestre = (dt.month - 1) // 3 + 1
            semana_anio = dt.isocalendar()[1]
            nombre_dia = dias[dt.weekday()]
            es_fin_de_semana = dt.weekday() >= 5
            
            records.append({
                'fecha_key': idx,
                'fecha': dt.date(),
                'anio': dt.year,
                'trimestre': trimestre,
                'mes': dt.month,
                'nombre_mes': meses[dt.month],
                'semana_anio': semana_anio,
                'dia': dt.day,
                'nombre_dia': nombre_dia,
                'es_fin_de_semana': es_fin_de_semana
            })
        
        df_dim = pd.DataFrame(records)
        logger.info(f"  ✓ Created {len(df_dim)} fecha records")
        return df_dim
    
    def create_dim_estado_orden(self) -> pd.DataFrame:
        """Create DimEstadoOrden dimension table."""
        logger.info("Creating DimEstadoOrden...")
        
        estados = [
            "pendiente",
            "pagada",
            "enviada",
            "entregada",
            "cancelada"
        ]
        
        df_dim = pd.DataFrame({
            'estado_key': range(1, len(estados) + 1),
            'estado': estados
        })
        
        logger.info(f"  ✓ Created {len(df_dim)} estado orden records")
        return df_dim
    
    def create_dim_metodo_pago(self) -> pd.DataFrame:
        """Create DimMetodoPago dimension table."""
        logger.info("Creating DimMetodoPago...")
        
        metodos = [
            "tarjeta_credito",
            "tarjeta_debito",
            "transferencia",
            "efectivo",
            "paypal"
        ]
        
        df_dim = pd.DataFrame({
            'metodo_pago_key': range(1, len(metodos) + 1),
            'metodo_pago': metodos
        })
        
        logger.info(f"  ✓ Created {len(df_dim)} metodo pago records")
        return df_dim
    
    def create_date_lookup(self, df_dim_fecha: pd.DataFrame) -> Dict[date, int]:
        """Create lookup dictionary for date keys."""
        return dict(zip(df_dim_fecha['fecha'], df_dim_fecha['fecha_key']))
    
    def create_status_lookup(self, df_dim_estado: pd.DataFrame) -> Dict[str, int]:
        """Create lookup dictionary for order status keys."""
        return dict(zip(df_dim_estado['estado'], df_dim_estado['estado_key']))
    
    def create_payment_lookup(self, df_dim_metodo: pd.DataFrame) -> Dict[str, int]:
        """Create lookup dictionary for payment method keys."""
        return dict(zip(df_dim_metodo['metodo_pago'], df_dim_metodo['metodo_pago_key']))
    
    def transform_fact_ventas(self,
                              df_ordenes: pd.DataFrame,
                              df_detalles: pd.DataFrame,
                              df_clientes: pd.DataFrame,
                              df_dim_cliente: pd.DataFrame,
                              df_dim_producto: pd.DataFrame,
                              df_dim_sucursal: pd.DataFrame,
                              df_dim_fecha: pd.DataFrame,
                              df_dim_estado: pd.DataFrame,
                              df_dim_metodo: pd.DataFrame) -> pd.DataFrame:
        """Transform transactional data into FactVentas."""
        logger.info("Transforming FactVentas...")
        
        # Create lookup dictionaries
        cliente_lookup = dict(zip(df_dim_cliente['cliente_id_natural'], df_dim_cliente['cliente_key']))
        producto_lookup = dict(zip(df_dim_producto['producto_id_natural'], df_dim_producto['producto_key']))
        sucursal_lookup = dict(zip(df_dim_sucursal['sucursal_id_natural'], df_dim_sucursal['sucursal_key']))
        date_lookup = self.create_date_lookup(df_dim_fecha)
        status_lookup = self.create_status_lookup(df_dim_estado)
        payment_lookup = self.create_payment_lookup(df_dim_metodo)
        
        # Merge order details with order header information
        df_merged = df_detalles.merge(
            df_ordenes[['id', 'cliente_id', 'sucursal_id', 'fecha_orden', 'estado', 'metodo_pago']],
            left_on='orden_id',
            right_on='id',
            how='left'
        )
        
        # Create fact table
        df_facts = pd.DataFrame()
        df_facts['orden_id'] = df_merged['orden_id']
        df_facts['detalle_orden_id'] = df_merged['id_x']  # DetalleOrden.id
        
        # Map to dimension keys
        df_facts['cliente_key'] = df_merged['cliente_id'].map(cliente_lookup)
        df_facts['producto_key'] = df_merged['producto_id'].map(producto_lookup)
        df_facts['sucursal_key'] = df_merged['sucursal_id'].map(sucursal_lookup)
        
        # Extract date and map to fecha_key
        df_merged['fecha_order_only'] = pd.to_datetime(df_merged['fecha_orden']).dt.date
        df_facts['fecha_key'] = df_merged['fecha_order_only'].map(date_lookup)
        
        # Handle missing dates (if order date not in dimension)
        if df_facts['fecha_key'].isna().any():
            logger.warning(f"  ⚠ {df_facts['fecha_key'].isna().sum()} orders have dates outside dimension range")
            # Use a default or fill with closest date
            df_facts['fecha_key'] = df_facts['fecha_key'].fillna(1)  # Fallback to first date
        
        # Map status and payment method
        df_facts['estado_key'] = df_merged['estado'].map(status_lookup)
        df_facts['metodo_pago_key'] = df_merged['metodo_pago'].map(payment_lookup)
        
        # Copy measures
        df_facts['cantidad'] = df_merged['cantidad']
        df_facts['precio_unitario'] = pd.to_numeric(df_merged['precio_unitario'], errors='coerce')
        df_facts['subtotal'] = pd.to_numeric(df_merged['subtotal'], errors='coerce')
        
        # Add surrogate key
        df_facts.insert(0, 'venta_key', range(1, len(df_facts) + 1))
        
        # Handle any remaining NaN values
        df_facts = df_facts.fillna(1)  # Default to key 1 for any missing lookups
        
        logger.info(f"  ✓ Transformed {len(df_facts)} fact records")
        return df_facts
    
    def load_data(self, target_conn: sqlite3.Connection, 
                  df: pd.DataFrame, table_name: str) -> None:
        """Load DataFrame into target database."""
        logger.info(f"Loading {table_name}...")
        
        df.to_sql(table_name, target_conn, if_exists='append', index=False)
        
        # Verify count
        count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", target_conn)['cnt'][0]
        logger.info(f"  ✓ Loaded {count} records into {table_name}")
    
    def run_pipeline(self, clear_existing: bool = True) -> None:
        """Execute the complete ETL pipeline."""
        logger.info("="*70)
        logger.info("STARTING ETL PIPELINE: TRANSACTIONAL TO STAR SCHEMA")
        logger.info("="*70)
        
        source_conn = None
        target_conn = None
        
        try:
            # Get connections
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            # Clear existing data if requested
            if clear_existing:
                self.clear_star_tables()
            
            # EXTRACT phase
            logger.info("\n" + "─"*70)
            logger.info("EXTRACT PHASE")
            logger.info("─"*70)
            
            df_clientes = self.extract_table(source_conn, "Cliente")
            df_productos = self.extract_table(source_conn, "Producto")
            df_sucursales = self.extract_table(source_conn, "Sucursal")
            df_ordenes = self.extract_table(source_conn, "Orden")
            df_detalles = self.extract_table(source_conn, "DetalleOrden")
            
            logger.info(f"\n✓ Extraction complete:")
            logger.info(f"  - {len(df_clientes)} clientes")
            logger.info(f"  - {len(df_productos)} productos")
            logger.info(f"  - {len(df_sucursales)} sucursales")
            logger.info(f"  - {len(df_ordenes)} órdenes")
            logger.info(f"  - {len(df_detalles)} detalles de orden")
            
            # TRANSFORM phase
            logger.info("\n" + "─"*70)
            logger.info("TRANSFORM PHASE")
            logger.info("─"*70)
            
            # Transform dimensions
            df_dim_cliente = self.transform_dim_cliente(df_clientes)
            df_dim_producto = self.transform_dim_producto(df_productos)
            df_dim_sucursal = self.transform_dim_sucursal(df_sucursales)
            
            # Create dimensions
            df_dim_fecha = self.create_dim_fecha()
            df_dim_estado = self.create_dim_estado_orden()
            df_dim_metodo = self.create_dim_metodo_pago()
            
            # Transform fact table
            df_fact_ventas = self.transform_fact_ventas(
                df_ordenes, df_detalles, df_clientes,
                df_dim_cliente, df_dim_producto, df_dim_sucursal,
                df_dim_fecha, df_dim_estado, df_dim_metodo
            )
            
            # LOAD phase
            logger.info("\n" + "─"*70)
            logger.info("LOAD PHASE")
            logger.info("─"*70)
            
            # Load dimensions first (foreign key dependencies)
            self.load_data(target_conn, df_dim_cliente, "DimCliente")
            self.load_data(target_conn, df_dim_producto, "DimProducto")
            self.load_data(target_conn, df_dim_sucursal, "DimSucursal")
            self.load_data(target_conn, df_dim_fecha, "DimFecha")
            self.load_data(target_conn, df_dim_estado, "DimEstadoOrden")
            self.load_data(target_conn, df_dim_metodo, "DimMetodoPago")
            
            # Load fact table
            self.load_data(target_conn, df_fact_ventas, "FactVentas")
            
            target_conn.commit()
            
            # Summary
            logger.info("\n" + "="*70)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
            logger.info("="*70)
            
            self.print_summary(target_conn)
            
        except Exception as e:
            logger.error(f"\n✗ Pipeline failed with error: {e}", exc_info=True)
            if target_conn:
                target_conn.rollback()
            raise
        
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()
    
    def print_summary(self, conn: sqlite3.Connection) -> None:
        """Print summary of loaded data."""
        logger.info("\nSTAR SCHEMA SUMMARY:")
        logger.info("─"*70)
        
        tables = [
            "DimCliente",
            "DimProducto",
            "DimSucursal",
            "DimFecha",
            "DimEstadoOrden",
            "DimMetodoPago",
            "FactVentas"
        ]
        
        for table in tables:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
                logger.info(f"  {table}: {count:,} records")
            except Exception as e:
                logger.warning(f"  {table}: Error retrieving count ({e})")
        
        logger.info("─"*70)


def main():
    """Main entry point."""
    try:
        pipeline = TransactionalToStarPipeline()
        pipeline.run_pipeline(clear_existing=True)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
