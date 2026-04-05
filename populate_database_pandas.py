"""
Database population script for transactional database.
Adds sample records to the database using pandas.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import random


DB_PATH = Path(__file__).parent / "db" / "transactional.sqlite"


def populate_database():
    """Populate the database with sample data using pandas."""
    
    if not DB_PATH.exists():
        print(f"✗ Database not found at: {DB_PATH}")
        print("Please run 'python create_database.py' first")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Create sample Clientes DataFrame
        clientes_data = {
            "nombre": ["Juan", "María", "Carlos", "Ana", "Roberto"],
            "apellido": ["Pérez", "García", "López", "Martínez", "Fernández"],
            "email": ["juan.perez@email.com", "maria.garcia@email.com", "carlos.lopez@email.com", 
                     "ana.martinez@email.com", "roberto.fernandez@email.com"],
            "telefono": ["5551234567", "5559876543", "5554567890", "5552345678", "5559988776"],
            "fecha_registro": [datetime.now()] * 5
        }
        df_clientes = pd.DataFrame(clientes_data)
        df_clientes.to_sql("Cliente", conn, if_exists="append", index=False)
        print(f"✓ Inserted {len(df_clientes)} clients")
        
        # Create sample Productos DataFrame
        productos_data = {
            "sku": ["SKU001", "SKU002", "SKU003", "SKU004", "SKU005", "SKU006", "SKU007", "SKU008"],
            "nombre": ["Laptop", "Monitor", "Teclado", "Mouse", "Headset", "Webcam", "SSD", "RAM"],
            "descripcion": ["Laptop Dell 15 pulgadas", "Monitor Samsung 27 pulgadas", 
                          "Teclado Mecánico RGB", "Mouse Logitech Inalámbrico",
                          "Headset Gamer Pro", "Webcam HD 1080p",
                          "SSD Samsung 1TB", "Memoria RAM 16GB DDR4"],
            "categoria": ["Electrónica", "Electrónica", "Accesorios", "Accesorios", 
                         "Audio", "Accesorios", "Almacenamiento", "Memoria"],
            "precio": [899.99, 299.99, 89.99, 45.99, 129.99, 59.99, 99.99, 69.99],
            "stock": [25, 40, 100, 150, 45, 60, 80, 120],
            "activo": [1] * 8,
            "fecha_creacion": [datetime.now()] * 8
        }
        df_productos = pd.DataFrame(productos_data)
        df_productos.to_sql("Producto", conn, if_exists="append", index=False)
        print(f"✓ Inserted {len(df_productos)} products")
        
        # Create sample Sucursales DataFrame
        sucursales_data = {
            "nombre": ["Sucursal Centro", "Sucursal Norte", "Sucursal Sur", 
                      "Sucursal Oriente", "Sucursal Monterrey"],
            "codigo_sucursal": ["SC001", "SN001", "SS001", "SO001", "SM001"],
            "calle": ["Avenida Paseo de la Reforma", "Boulevard Manuel Ávila Camacho",
                     "Avenida Universidad", "Anillo Periférico", "Avenida Constitución"],
            "numero": ["505", "2000", "1500", "3000", "1000"],
            "colonia": ["Cuauhtémoc", "Chapultepec", "Oxtopulco", "Iztacalco", "Centro"],
            "ciudad": ["Ciudad de México", "Ciudad de México", "Ciudad de México", 
                      "Ciudad de México", "Monterrey"],
            "estado": ["México", "México", "México", "México", "Nuevo León"],
            "pais": ["México"] * 5,
            "codigo_postal": ["06500", "11570", "04000", "08200", "64000"],
            "latitud": [19.4326, 19.4372, 19.3256, 19.4169, 25.6866],
            "longitud": [-99.1332, -99.1659, -99.1789, -99.0834, -100.3161]
        }
        df_sucursales = pd.DataFrame(sucursales_data)
        df_sucursales.to_sql("Sucursal", conn, if_exists="append", index=False)
        print(f"✓ Inserted {len(df_sucursales)} branches")
        
        # Create sample Órdenes DataFrame
        estados_orden = ["pendiente", "pagada", "enviada", "entregada", "cancelada"]
        metodos_pago = ["tarjeta_credito", "tarjeta_debito", "transferencia", "efectivo", "paypal"]
        
        ordenes_data = {
            "cliente_id": [random.randint(1, 5) for _ in range(15)],
            "sucursal_id": [random.randint(1, 5) for _ in range(15)],
            "fecha_orden": [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(15)],
            "estado": [random.choice(estados_orden) for _ in range(15)],
            "metodo_pago": [random.choice(metodos_pago) for _ in range(15)],
            "total": [0.00] * 15  # Will be updated after inserting order details
        }
        df_ordenes = pd.DataFrame(ordenes_data)
        df_ordenes.to_sql("Orden", conn, if_exists="append", index=False)
        print(f"✓ Inserted {len(df_ordenes)} orders")
        
        # Create sample DetalleOrden DataFrame and update order totals
        detalles_data = {
            "orden_id": [],
            "producto_id": [],
            "cantidad": [],
            "precio_unitario": [],
            "subtotal": []
        }
        
        # Get producto prices for lookup
        df_productos_db = pd.read_sql("SELECT id, precio FROM Producto", conn)
        precio_dict = dict(zip(df_productos_db["id"], df_productos_db["precio"]))
        
        # Generate order details
        ordenes_totals = {}
        for orden_id in range(1, len(df_ordenes) + 1):
            num_items = random.randint(1, 4)
            total_orden = 0
            
            for _ in range(num_items):
                producto_id = random.randint(1, len(df_productos))
                cantidad = random.randint(1, 5)
                precio_unitario = precio_dict[producto_id]
                subtotal = cantidad * precio_unitario
                total_orden += subtotal
                
                detalles_data["orden_id"].append(orden_id)
                detalles_data["producto_id"].append(producto_id)
                detalles_data["cantidad"].append(cantidad)
                detalles_data["precio_unitario"].append(precio_unitario)
                detalles_data["subtotal"].append(subtotal)
            
            ordenes_totals[orden_id] = total_orden
        
        df_detalles = pd.DataFrame(detalles_data)
        df_detalles.to_sql("DetalleOrden", conn, if_exists="append", index=False)
        print(f"✓ Inserted {len(df_detalles)} order details")
        
        # Update order totals
        for orden_id, total in ordenes_totals.items():
            cursor = conn.cursor()
            cursor.execute("UPDATE Orden SET total = ? WHERE id = ?", (total, orden_id))
        
        conn.commit()
        print("\n✓ Database successfully populated with sample data!")
        
        # Display summary using pandas
        print("\n" + "="*50)
        print("DATABASE SUMMARY")
        print("="*50)
        print(f"Clients: {pd.read_sql('SELECT COUNT(*) as count FROM Cliente', conn)['count'][0]}")
        print(f"Products: {pd.read_sql('SELECT COUNT(*) as count FROM Producto', conn)['count'][0]}")
        print(f"Branches: {pd.read_sql('SELECT COUNT(*) as count FROM Sucursal', conn)['count'][0]}")
        print(f"Orders: {pd.read_sql('SELECT COUNT(*) as count FROM Orden', conn)['count'][0]}")
        print(f"Order Details: {pd.read_sql('SELECT COUNT(*) as count FROM DetalleOrden', conn)['count'][0]}")
        print("="*50)
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error populating database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    populate_database()
