"""
Database query utility for the transactional database.
Provides helper functions to query and display data.
"""

import sqlite3
from pathlib import Path
from tabulate import tabulate


DB_PATH_TRANSACTIONAL = Path(__file__).parent / "db" / "transactional.sqlite"
DB_PATH_STAR_SCHEMA = Path(__file__).parent / "db" / "star_schema.sqlite"


def get_connection(DB_PATH=DB_PATH_TRANSACTIONAL):
    """Get database connection."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def display_table(table_name):
    """Display all records from a table."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        print(f"\n{'='*80}")
        print(f"TABLE: {table_name.upper()}")
        print(f"{'='*80}")
        
        if rows:
            print(tabulate(rows, headers=columns, tablefmt="grid"))
            print(f"\nTotal records: {len(rows)}\n")
        else:
            print(f"No records found in {table_name}\n")
            
    except sqlite3.Error as e:
        print(f"Error querying table: {e}")
    finally:
        conn.close()


def get_orders_with_details():
    """Get orders with client and branch information."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                o.id,
                c.nombre || ' ' || c.apellido as 'Cliente',
                s.nombre as 'Sucursal',
                o.fecha_orden,
                o.estado,
                o.total
            FROM Orden o
            JOIN Cliente c ON o.cliente_id = c.id
            JOIN Sucursal s ON o.sucursal_id = s.id
            ORDER BY o.fecha_orden DESC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        print(f"\n{'='*100}")
        print("ÓRDENES CON DETALLES DEL CLIENTE Y SUCURSAL")
        print(f"{'='*100}")
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print(f"\nTotal órdenes: {len(rows)}\n")
        
    except sqlite3.Error as e:
        print(f"Error querying orders: {e}")
    finally:
        conn.close()


def get_order_details(order_id):
    """Get detailed information for a specific order."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get order header
        cursor.execute("""
            SELECT 
                o.id,
                c.nombre || ' ' || c.apellido as 'Cliente',
                s.nombre as 'Sucursal',
                o.fecha_orden,
                o.estado,
                o.metodo_pago,
                o.total
            FROM Orden o
            JOIN Cliente c ON o.cliente_id = c.id
            JOIN Sucursal s ON o.sucursal_id = s.id
            WHERE o.id = ?
        """, (order_id,))
        
        order = cursor.fetchone()
        if not order:
            print(f"Order {order_id} not found")
            return
        
        print(f"\n{'='*80}")
        print(f"ORDEN #{order[0]}")
        print(f"{'='*80}")
        print(f"Cliente: {order[1]}")
        print(f"Sucursal: {order[2]}")
        print(f"Fecha: {order[3]}")
        print(f"Estado: {order[4]}")
        print(f"Método de Pago: {order[5]}")
        print(f"Total: ${order[6]:.2f}")
        print(f"{'='*80}")
        
        # Get order items
        cursor.execute("""
            SELECT 
                p.nombre,
                do.cantidad,
                do.precio_unitario,
                do.subtotal
            FROM DetalleOrden do
            JOIN Producto p ON do.producto_id = p.id
            WHERE do.orden_id = ?
        """, (order_id,))
        
        columns = ['Producto', 'Cantidad', 'Precio Unitario', 'Subtotal']
        rows = cursor.fetchall()
        
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print()
        
    except sqlite3.Error as e:
        print(f"Error querying order details: {e}")
    finally:
        conn.close()


def get_sales_summary():
    """Get sales summary by branch."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                s.nombre as 'Sucursal',
                COUNT(o.id) as 'Total de Órdenes',
                COUNT(DISTINCT o.cliente_id) as 'Clientes Únicos',
                SUM(o.total) as 'Monto Total',
                AVG(o.total) as 'Promedio por Orden'
            FROM Sucursal s
            LEFT JOIN Orden o ON s.id = o.sucursal_id
            GROUP BY s.id
            ORDER BY SUM(o.total) DESC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        print(f"\n{'='*100}")
        print("RESUMEN DE VENTAS POR SUCURSAL")
        print(f"{'='*100}")
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print()
        
    except sqlite3.Error as e:
        print(f"Error querying sales summary: {e}")
    finally:
        conn.close()




def get_most_selled_products():
    """Get most selled products."""
    conn_transactional = get_connection(DB_PATH=DB_PATH_TRANSACTIONAL)
    cursor_transactional = conn_transactional.cursor()
    
    try:
        cursor_transactional.execute("""
            SELECT 
                p.nombre as 'Producto',
                SUM(do.cantidad) as 'Cantidad Vendida',
                SUM(do.subtotal) as 'Monto Total'
            FROM DetalleOrden do
            JOIN Producto p ON do.producto_id = p.id
            GROUP BY p.id
            ORDER BY SUM(do.cantidad) DESC
            LIMIT 10
        """)
        
        columns = [desc[0] for desc in cursor_transactional.description]
        rows = cursor_transactional.fetchall()
        
        print(f"\n{'='*80}")
        print("TOP 10 PRODUCTOS MÁS VENDIDOS")
        print(f"{'='*80}")
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print()
        
    except sqlite3.Error as e:
        print(f"Error querying most selled products: {e}")
    finally:
        conn_transactional.close()

def show_fact_ventas():
    """Show all records from FactVentas."""
    conn_star = get_connection(DB_PATH=DB_PATH_STAR_SCHEMA)
    cursor_star = conn_star.cursor()
    
    try:
        cursor_star.execute("SELECT * FROM FactVentas")
        columns = [desc[0] for desc in cursor_star.description]
        rows = cursor_star.fetchall()
        
        print(f"\n{'='*80}")
        print("FACTVENTAS - ALL RECORDS")
        print(f"{'='*80}")
        
        if rows:
            print(tabulate(rows, headers=columns, tablefmt="grid"))
            print(f"\nTotal records: {len(rows)}\n")
        else:
            print("No records found in FactVentas\n")
            
    except sqlite3.Error as e:
        print(f"Error querying FactVentas: {e}")
    finally:
        conn_star.close()

def dimensional_most_selled_product_by_branch():
    """Get most selled products by branch."""
    conn_star = get_connection(DB_PATH=DB_PATH_STAR_SCHEMA)
    cursor_star = conn_star.cursor()
    
    try:
        cursor_star.execute("""
            SELECT 
                s.nombre as 'Sucursal',
                p.nombre as 'Producto',
                SUM(f.cantidad) as 'Cantidad Vendida',
                SUM(f.subtotal) as 'Monto Total'
            FROM FactVentas f
            JOIN DimSucursal s ON f.sucursal_key = s.sucursal_key
            JOIN DimProducto p ON f.producto_key = p.producto_key
            GROUP BY s.sucursal_key, p.producto_key
            ORDER BY SUM(f.cantidad) DESC
        """)
        
        columns = [desc[0] for desc in cursor_star.description]
        rows = cursor_star.fetchall()
        
        print(f"\n{'='*100}")
        print("PRODUCTOS MÁS VENDIDOS POR SUCURSAL")
        print(f"{'='*100}")
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print()
        
    except sqlite3.Error as e:
        print(f"Error querying most selled products by branch: {e}")
    finally:
        conn_star.close()

def amount_selled_per_day_of_the_week():
    """Get amount selled per day of the week."""
    conn_star = get_connection(DB_PATH=DB_PATH_STAR_SCHEMA)
    cursor_star = conn_star.cursor()
    
    try:
        cursor_star.execute("""
            SELECT 
                strftime('%w', f.fecha_orden) as 'Día de la Semana',
                SUM(f.subtotal) as 'Monto Total'
            FROM FactVentas f
            GROUP BY strftime('%w', f.fecha_orden)
            ORDER BY SUM(f.subtotal) DESC
        """)
        
        columns = [desc[0] for desc in cursor_star.description]
        rows = cursor_star.fetchall()
        
        print(f"\n{'='*80}")
        print("MONTO VENDIDO POR DÍA DE LA SEMANA")
        print(f"{'='*80}")
        print(tabulate(rows, headers=columns, tablefmt="grid"))
        print()
        
    except sqlite3.Error as e:
        print(f"Error querying amount selled per day of the week: {e}")
    finally:
        conn_star.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "clientes":
            display_table("Cliente")
        elif command == "productos":
            display_table("Producto")
        elif command == "sucursales":
            display_table("Sucursal")
        elif command == "ordenes":
            display_table("Orden")
        elif command == "detalles":
            display_table("DetalleOrden")
        elif command == "ordenes-completas":
            get_orders_with_details()
        elif command == "resumen":
            get_sales_summary()
        elif command == "orden" and len(sys.argv) > 2:
            try:
                order_id = int(sys.argv[2])
                get_order_details(order_id)
            except ValueError:
                print("Invalid order ID")
        elif command == "top-productos":
            get_most_selled_products()

#################################################### Analytical queries for star schema ####################################################
        elif command == "show-fact-ventas":
            show_fact_ventas()
        elif command == "top-productos-por-sucursal":
            dimensional_most_selled_product_by_branch()
        elif command == "monto-por-dia-semana":
            amount_selled_per_day_of_the_week()
        else:
            print("Available commands:")
            print("  python query_database.py clientes       - Show all clients")
            print("  python query_database.py productos      - Show all products")
            print("  python query_database.py sucursales     - Show all branches")
            print("  python query_database.py ordenes        - Show all orders")
            print("  python query_database.py detalles       - Show all order details")
            print("  python query_database.py ordenes-completas - Show orders with client/branch info")
            print("  python query_database.py resumen        - Show sales summary by branch")
            print("  python query_database.py orden <id>     - Show details for specific order")
            print("  python query_database.py top-productos  - Show top 10 most selled products")
            print("  python query_database.py show-fact-ventas - Show all records from FactVentas")
            print("  python query_database.py top-productos-por-sucursal - Show most selled products by branch")
            print("  python query_database.py monto-por-dia-semana - Show amount selled per day of the week")
    else:
        # Display all data
        print("DATABASE QUERY UTILITY")
        print("Use 'python query_database.py <command>' to query the database")
        print("\nAvailable commands:")
        print("  clientes          - Show all clients")
        print("  productos         - Show all products")
        print("  sucursales        - Show all branches")
        print("  ordenes           - Show all orders")
        print("  detalles          - Show all order details")
        print("  ordenes-completas - Show orders with client/branch info")
        print("  resumen           - Show sales summary by branch")
        print("  orden <id>        - Show details for specific order")
        print("  top-productos     - Show top 10 most selled products")
        print("  show-fact-ventas  - Show all records from FactVentas")
        print("  top-productos-por-sucursal - Show most selled products by branch")
        print("  monto-por-dia-semana - Show amount selled per day of the week")

