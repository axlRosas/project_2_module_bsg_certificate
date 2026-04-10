# Transactional Database Setup Guide

This directory contains all necessary scripts to create and manage a transactional SQLite database based on the DBML structure defined in `dbml_structure_transactional.txt`.

## 📁 Project Structure

```
.
├── create_database.py        # Script to create the database and tables
├── populate_database.py      # Script to insert sample data
├── query_database.py         # Utility script for querying the database
├── db/
│   └── transactional.sqlite  # SQLite database file (created automatically)
└── README.md                 # This file
```

## 🚀 Quick Start

### Step 1: Create the Database

First, ensure your virtual environment is activated, then run:

```bash
source .venv/bin/activate
python create_database.py
```

This script will:
- Create the `db/` directory if it doesn't exist
- Check if `transactional.sqlite` already exists
- If not, create the database with all 5 tables:
  - **Cliente** - Customer information
  - **Producto** - Product catalog
  - **Sucursal** - Branch locations
  - **Orden** - Sales orders
  - **DetalleOrden** - Order line items

### Step 2: Populate with Sample Data

After creating the database, populate it with sample records:

```bash
python populate_database.py
```

This script will:
- Create 5 sample clients
- Create 8 sample products
- Create 5 sample branch locations
- Create 15 sample orders with random order details
- Display a summary of inserted data

**Output example:**
```
✓ Inserted 5 clients
✓ Inserted 8 products
✓ Inserted 5 branches
✓ Inserted 15 orders
✓ Inserted order details for all orders

==================================================
DATABASE SUMMARY
==================================================
Clients: 5
Products: 8
Branches: 5
Orders: 15
Order Details: 44
==================================================
```

## 📊 Database Schema

### Cliente (Clients)
| Field | Type | Constraints |
|-------|------|-------------|
| id | INTEGER | PRIMARY KEY, AUTO INCREMENT |
| nombre | VARCHAR(100) | NOT NULL |
| apellido | VARCHAR(100) | NOT NULL |
| email | VARCHAR(150) | NOT NULL, UNIQUE |
| telefono | VARCHAR(20) | Optional |
| fecha_registro | DATETIME | NOT NULL |

### Producto (Products)
| Field | Type | Constraints |
|-------|------|-------------|
| id | INTEGER | PRIMARY KEY, AUTO INCREMENT |
| sku | VARCHAR(50) | NOT NULL, UNIQUE |
| nombre | VARCHAR(150) | NOT NULL |
| descripcion | TEXT | Optional |
| categoria | VARCHAR(100) | NOT NULL |
| precio | DECIMAL(10,2) | NOT NULL |
| stock | INTEGER | NOT NULL, DEFAULT: 0 |
| activo | BOOLEAN | NOT NULL, DEFAULT: true |
| fecha_creacion | DATETIME | NOT NULL |

### Sucursal (Branches)
| Field | Type | Constraints |
|-------|------|-------------|
| id | INTEGER | PRIMARY KEY, AUTO INCREMENT |
| nombre | VARCHAR(120) | NOT NULL |
| codigo_sucursal | VARCHAR(30) | NOT NULL, UNIQUE |
| calle | VARCHAR(150) | NOT NULL |
| numero | VARCHAR(20) | Optional |
| colonia | VARCHAR(100) | Optional |
| ciudad | VARCHAR(100) | NOT NULL |
| estado | VARCHAR(100) | NOT NULL |
| pais | VARCHAR(100) | NOT NULL, DEFAULT: 'México' |
| codigo_postal | VARCHAR(10) | Optional |
| latitud | DECIMAL(9,6) | Optional |
| longitud | DECIMAL(9,6) | Optional |

### Orden (Orders)
| Field | Type | Constraints |
|-------|------|-------------|
| id | INTEGER | PRIMARY KEY, AUTO INCREMENT |
| cliente_id | INTEGER | NOT NULL, FK → Cliente.id |
| sucursal_id | INTEGER | NOT NULL, FK → Sucursal.id |
| fecha_orden | DATETIME | NOT NULL |
| estado | VARCHAR(50) | NOT NULL, DEFAULT: 'pendiente' |
| metodo_pago | VARCHAR(50) | Optional |
| total | DECIMAL(12,2) | NOT NULL |

**Valid estado values:** pendiente, pagada, enviada, entregada, cancelada

### DetalleOrden (Order Details)
| Field | Type | Constraints |
|-------|------|-------------|
| id | INTEGER | PRIMARY KEY, AUTO INCREMENT |
| orden_id | INTEGER | NOT NULL, FK → Orden.id |
| producto_id | INTEGER | NOT NULL, FK → Producto.id |
| cantidad | INTEGER | NOT NULL |
| precio_unitario | DECIMAL(10,2) | NOT NULL |
| subtotal | DECIMAL(12,2) | NOT NULL |

## 🔍 Querying the Database

Use the `query_database.py` utility script to query the database:

```bash
# Show all clients
python query_database.py clientes

# Show all products
python query_database.py productos

# Show all branches
python query_database.py sucursales

# Show all orders
python query_database.py ordenes

# Show all order details
python query_database.py detalles

# Show orders with client and branch information
python query_database.py ordenes-completas

# Show sales summary by branch
python query_database.py resumen

# Show details for a specific order (e.g., order #1)
python query_database.py orden 1
```

## 💾 Database Location

The SQLite database file is stored at:
```
./db/transactional.sqlite
```

You can also access it with any SQLite client:
```bash
sqlite3 db/transactional.sqlite
```

## 🔄 Re-initialize Database

To reset and recreate the database:

```bash
# Delete the existing database
rm db/transactional.sqlite

# Recreate it
python create_database.py

# Repopulate with sample data
python populate_database.py
```

## ✅ Verification

You can verify the database was created correctly by:

1. Checking the file exists:
   ```bash
   ls -lh db/transactional.sqlite
   ```

2. Checking tables were created:
   ```bash
   sqlite3 db/transactional.sqlite ".tables"
   ```

3. Using the query utility:
   ```bash
   python query_database.py ordenes-completas
   ```

## 📝 Notes

- The scripts use Python's built-in `sqlite3` module (no additional dependencies needed)
- The `query_database.py` script requires the `tabulate` package for nice table formatting. Install it with:
  ```bash
  pip install tabulate
  ```
- All timestamps use the `datetime.now()` format
- Foreign key constraints are enforced
- The database automatically handles auto-increment primary keys

## 🆘 Troubleshooting

### Database file not found
Make sure you've run `create_database.py` first.

### Import errors
Ensure your virtual environment is activated:
```bash
source .venv/bin/activate
```

### Permission denied
Check file permissions:
```bash
chmod 755 db/
```

### Table already exists error
You can safely ignore this - the `create_database.py` script checks if the database exists before creating it.
