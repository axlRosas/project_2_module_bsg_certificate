"""
Database population script for transactional database.
Adds sample records to the database.
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import random


DB_PATH = Path(__file__).parent / "db" / "transactional.sqlite"


def populate_database():
    """Populate the database with sample data."""
    
    if not DB_PATH.exists():
        print(f"✗ Database not found at: {DB_PATH}")
        print("Please run 'python create_database.py' first")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Insert sample Clientes
        clientes = [
            ("Juan", "Pérez", "juan.perez@email.com", "5551234567"),
            ("María", "García", "maria.garcia@email.com", "5559876543"),
            ("Carlos", "López", "carlos.lopez@email.com", "5554567890"),
            ("Ana", "Martínez", "ana.martinez@email.com", "5552345678"),
            ("Roberto", "Fernández", "roberto.fernandez@email.com", "5559988776"),
        ]
        
        for nombre, apellido, email, telefono in clientes:
            cursor.execute("""
                INSERT INTO Cliente (nombre, apellido, email, telefono, fecha_registro)
                VALUES (?, ?, ?, ?, ?)
            """, (nombre, apellido, email, telefono, datetime.now()))
        
        print(f"✓ Inserted {len(clientes)} clients")
        
        # Insert sample Productos
        productos = [
            ("SKU001", "Laptop", "Laptop Dell 15 pulgadas", "Electrónica", 899.99, 25),
            ("SKU002", "Monitor", "Monitor Samsung 27 pulgadas", "Electrónica", 299.99, 40),
            ("SKU003", "Teclado", "Teclado Mecánico RGB", "Accesorios", 89.99, 100),
            ("SKU004", "Mouse", "Mouse Logitech Inalámbrico", "Accesorios", 45.99, 150),
            ("SKU005", "Headset", "Headset Gamer Pro", "Audio", 129.99, 45),
            ("SKU006", "Webcam", "Webcam HD 1080p", "Accesorios", 59.99, 60),
            ("SKU007", "SSD", "SSD Samsung 1TB", "Almacenamiento", 99.99, 80),
            ("SKU008", "RAM", "Memoria RAM 16GB DDR4", "Memoria", 69.99, 120),
        ]
        
        for sku, nombre, descripcion, categoria, precio, stock in productos:
            cursor.execute("""
                INSERT INTO Producto (sku, nombre, descripcion, categoria, precio, stock, activo, fecha_creacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (sku, nombre, descripcion, categoria, precio, stock, True, datetime.now()))
        
        print(f"✓ Inserted {len(productos)} products")
        
        # Insert sample Sucursales
        sucursales = [
            ("Sucursal Centro", "SC001", "Avenida Paseo de la Reforma", "505", "Cuauhtémoc", "México", "Ciudad de México", "06500"),
            ("Sucursal Norte", "SN001", "Boulevard Manuel Ávila Camacho", "2000", "Chapultepec", "México", "Ciudad de México", "11570"),
            ("Sucursal Sur", "SS001", "Avenida Universidad", "1500", "Oxtopulco", "México", "Ciudad de México", "04000"),
            ("Sucursal Oriente", "SO001", "Anillo Periférico", "3000", "Iztacalco", "México", "Ciudad de México", "08200"),
            ("Sucursal Monterrey", "SM001", "Avenida Constitución", "1000", "Centro", "Nuevo León", "Monterrey", "64000"),
        ]
        
        for nombre, codigo, calle, numero, colonia, estado, ciudad, cp in sucursales:
            cursor.execute("""
                INSERT INTO Sucursal (nombre, codigo_sucursal, calle, numero, colonia, ciudad, estado, pais, codigo_postal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (nombre, codigo, calle, numero, colonia, ciudad, estado, "México", cp))
        
        print(f"✓ Inserted {len(sucursales)} branches")
        
        # Insert sample Órdenes
        estados_orden = ["pendiente", "pagada", "enviada", "entregada", "cancelada"]
        metodos_pago = ["tarjeta_credito", "tarjeta_debito", "transferencia", "efectivo", "paypal"]
        
        for i in range(15):
            cliente_id = random.randint(1, len(clientes))
            sucursal_id = random.randint(1, len(sucursales))
            estado = random.choice(estados_orden)
            metodo_pago = random.choice(metodos_pago)
            fecha_orden = datetime.now() - timedelta(days=random.randint(1, 30))
            
            cursor.execute("""
                INSERT INTO Orden (cliente_id, sucursal_id, fecha_orden, estado, metodo_pago, total)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (cliente_id, sucursal_id, fecha_orden, estado, metodo_pago, 0.00))
        
        print(f"✓ Inserted 15 orders")
        
        # Insert sample DetalleOrden and update order totals
        cursor.execute("SELECT id FROM Orden")
        ordenes = cursor.fetchall()
        
        for orden_id in ordenes:
            orden_id = orden_id[0]
            num_items = random.randint(1, 4)
            total_orden = 0
            
            for _ in range(num_items):
                producto_id = random.randint(1, len(productos))
                cantidad = random.randint(1, 5)
                
                # Get product price
                cursor.execute("SELECT precio FROM Producto WHERE id = ?", (producto_id,))
                precio_unitario = cursor.fetchone()[0]
                subtotal = cantidad * precio_unitario
                total_orden += subtotal
                
                cursor.execute("""
                    INSERT INTO DetalleOrden (orden_id, producto_id, cantidad, precio_unitario, subtotal)
                    VALUES (?, ?, ?, ?, ?)
                """, (orden_id, producto_id, cantidad, precio_unitario, subtotal))
            
            # Update order total
            cursor.execute("UPDATE Orden SET total = ? WHERE id = ?", (total_orden, orden_id))
        
        print(f"✓ Inserted order details for all orders")
        
        # Commit all changes
        conn.commit()
        print("\n✓ Database successfully populated with sample data!")
        
        # Display summary
        print("\n" + "="*50)
        print("DATABASE SUMMARY")
        print("="*50)
        cursor.execute("SELECT COUNT(*) FROM Cliente")
        print(f"Clients: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Producto")
        print(f"Products: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Sucursal")
        print(f"Branches: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM Orden")
        print(f"Orders: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM DetalleOrden")
        print(f"Order Details: {cursor.fetchone()[0]}")
        print("="*50)
        
    except sqlite3.Error as e:
        conn.rollback()
        print(f"✗ Error populating database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    populate_database()
