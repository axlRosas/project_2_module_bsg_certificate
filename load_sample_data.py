"""
Script to load sample data from JSON file into the transactional database
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import random


def load_json_data():
    """Load sample data from JSON file into the database"""
    
    json_file = Path(__file__).parent / "sample_data.json"
    db_file = Path(__file__).parent / "db" / "transactional.sqlite"
    
    if not json_file.exists():
        print(f"✗ JSON file not found at: {json_file}")
        return
    
    if not db_file.exists():
        print(f"✗ Database not found at: {db_file}")
        print("Please run 'python create_database.py' first")
        return
    
    # Load JSON data
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("="*70)
    print("LOADING SAMPLE DATA FROM JSON")
    print("="*70)
    
    # Connect to database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    try:
        # Clear existing data (optional - uncomment to reset)
        # cursor.execute("DELETE FROM DetalleOrden")
        # cursor.execute("DELETE FROM Orden")
        # cursor.execute("DELETE FROM Producto")
        # cursor.execute("DELETE FROM Sucursal")
        # cursor.execute("DELETE FROM Cliente")
        
        # Load Clientes
        print("\n--- Loading Clientes ---")
        for cliente in data['clientes']:
            cursor.execute("""
                INSERT OR IGNORE INTO Cliente (nombre, apellido, email, telefono, fecha_registro)
                VALUES (?, ?, ?, ?, ?)
            """, (
                cliente['nombre'],
                cliente['apellido'],
                cliente['email'],
                cliente['telefono'],
                datetime.now()
            ))
            print(f"  ✓ {cliente['nombre']} {cliente['apellido']}")
        
        # Load Productos
        print("\n--- Loading Productos ---")
        for producto in data['productos']:
            cursor.execute("""
                INSERT OR IGNORE INTO Producto (sku, nombre, descripcion, categoria, precio, stock, activo, fecha_creacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                producto['sku'],
                producto['nombre'],
                producto.get('descripcion', ''),
                producto['categoria'],
                producto['precio'],
                producto['stock'],
                True,
                datetime.now()
            ))
            print(f"  ✓ {producto['nombre']} (SKU: {producto['sku']})")
        
        # Load Sucursales
        print("\n--- Loading Sucursales ---")
        for sucursal in data['sucursales']:
            cursor.execute("""
                INSERT OR IGNORE INTO Sucursal (nombre, codigo_sucursal, calle, numero, colonia, ciudad, estado, pais, codigo_postal, latitud, longitud)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sucursal['nombre'],
                sucursal['codigo_sucursal'],
                sucursal['calle'],
                sucursal['numero'],
                sucursal['colonia'],
                sucursal['ciudad'],
                sucursal['estado'],
                'México',
                sucursal['codigo_postal'],
                sucursal['latitud'],
                sucursal['longitud']
            ))
            print(f"  ✓ {sucursal['nombre']} ({sucursal['codigo_sucursal']})")
        
        conn.commit()
        
        # Load Órdenes and Detalles
        print("\n--- Loading Órdenes and DetalleOrdenes ---")
        orden_count = 0
        detalle_count = 0
        
        for idx, orden in enumerate(data['ordenes']):
            # Generate random date within last 30 days
            fecha_orden = datetime.now() - timedelta(days=random.randint(1, 30))
            
            # Insert orden
            cursor.execute("""
                INSERT INTO Orden (cliente_id, sucursal_id, fecha_orden, estado, metodo_pago, total)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                orden['cliente_id'],
                orden['sucursal_id'],
                fecha_orden,
                orden['estado'],
                orden['metodo_pago'],
                0.00  # Will be updated after inserting details
            ))
            
            orden_id = cursor.lastrowid
            orden_count += 1
            
            # Insert detalles and calculate total
            total_orden = 0
            for detalle in orden['detalles']:
                subtotal = detalle['cantidad'] * detalle['precio_unitario']
                total_orden += subtotal
                
                cursor.execute("""
                    INSERT INTO DetalleOrden (orden_id, producto_id, cantidad, precio_unitario, subtotal)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    orden_id,
                    detalle['producto_id'],
                    detalle['cantidad'],
                    detalle['precio_unitario'],
                    subtotal
                ))
                detalle_count += 1
            
            # Update orden total
            cursor.execute("UPDATE Orden SET total = ? WHERE id = ?", (total_orden, orden_id))
            
            print(f"  ✓ Orden #{orden_id} - {orden['estado']} (${total_orden:.2f})")
        
        conn.commit()
        
        # Display summary
        print("\n" + "="*70)
        print("LOAD SUMMARY")
        print("="*70)
        
        cursor.execute("SELECT COUNT(*) FROM Cliente")
        print(f"Clientes: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Producto")
        print(f"Productos: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Sucursal")
        print(f"Sucursales: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Orden")
        print(f"Órdenes: {cursor.fetchone()[0]} (loaded {orden_count} new)")
        cursor.execute("SELECT COUNT(*) FROM DetalleOrden")
        print(f"Detalles de Orden: {cursor.fetchone()[0]} (loaded {detalle_count} new)")
        
        # Show sales summary
        cursor.execute("""
            SELECT 
                COUNT(*) as total_ordenes,
                SUM(total) as monto_total,
                AVG(total) as promedio
            FROM Orden
        """)
        result = cursor.fetchone()
        print(f"\nVentas Totales: ${result[1]:.2f}")
        print(f"Promedio por Orden: ${result[2]:.2f}")
        
        print("\n✓ Data loaded successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error loading data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    load_json_data()
