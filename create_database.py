"""
Database initialization script for transactional database.
Creates SQLite database and all tables based on DBML structure.
"""

import sqlite3
import os
from pathlib import Path


DB_PATH = Path(__file__).parent / "db" / "transactional.sqlite"


def create_database():
    """Create the SQLite database and initialize all tables."""
    
    # Ensure db directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if database already exists
    if DB_PATH.exists():
        print(f"✓ Database already exists at: {DB_PATH}")
        return
    
    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Create Cliente table
        cursor.execute("""
            CREATE TABLE Cliente (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre VARCHAR(100) NOT NULL,
                apellido VARCHAR(100) NOT NULL,
                email VARCHAR(150) NOT NULL UNIQUE,
                telefono VARCHAR(20),
                fecha_registro DATETIME NOT NULL
            )
        """)
        print("✓ Created Cliente table")
        
        # Create Producto table
        cursor.execute("""
            CREATE TABLE Producto (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku VARCHAR(50) NOT NULL UNIQUE,
                nombre VARCHAR(150) NOT NULL,
                descripcion TEXT,
                categoria VARCHAR(100) NOT NULL,
                precio DECIMAL(10,2) NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                activo BOOLEAN NOT NULL DEFAULT 1,
                fecha_creacion DATETIME NOT NULL
            )
        """)
        print("✓ Created Producto table")
        
        # Create Sucursal table
        cursor.execute("""
            CREATE TABLE Sucursal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre VARCHAR(120) NOT NULL,
                codigo_sucursal VARCHAR(30) NOT NULL UNIQUE,
                calle VARCHAR(150) NOT NULL,
                numero VARCHAR(20),
                colonia VARCHAR(100),
                ciudad VARCHAR(100) NOT NULL,
                estado VARCHAR(100) NOT NULL,
                pais VARCHAR(100) NOT NULL DEFAULT 'México',
                codigo_postal VARCHAR(10),
                latitud DECIMAL(9,6),
                longitud DECIMAL(9,6)
            )
        """)
        print("✓ Created Sucursal table")
        
        # Create Orden table
        cursor.execute("""
            CREATE TABLE Orden (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_id INTEGER NOT NULL,
                sucursal_id INTEGER NOT NULL,
                fecha_orden DATETIME NOT NULL,
                estado VARCHAR(50) NOT NULL DEFAULT 'pendiente',
                metodo_pago VARCHAR(50),
                total DECIMAL(12,2) NOT NULL,
                FOREIGN KEY (cliente_id) REFERENCES Cliente(id),
                FOREIGN KEY (sucursal_id) REFERENCES Sucursal(id)
            )
        """)
        print("✓ Created Orden table")
        
        # Create DetalleOrden table
        cursor.execute("""
            CREATE TABLE DetalleOrden (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                orden_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                precio_unitario DECIMAL(10,2) NOT NULL,
                subtotal DECIMAL(12,2) NOT NULL,
                FOREIGN KEY (orden_id) REFERENCES Orden(id),
                FOREIGN KEY (producto_id) REFERENCES Producto(id)
            )
        """)
        print("✓ Created DetalleOrden table")
        
        # Commit changes
        conn.commit()
        print(f"\n✓ Database successfully created at: {DB_PATH}")
        
    except sqlite3.Error as e:
        conn.rollback()
        print(f"✗ Error creating database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    create_database()
