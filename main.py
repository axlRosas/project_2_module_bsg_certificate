#!/usr/bin/env python3
"""
Main Orchestration File - Database Setup, Schema Creation, and Analytics
============================================================================

This script executes the complete workflow:
1. Create the transactional database
2. Display the transactional schema structure
3. Load sample data
4. Run ETL pipeline to create star schema
5. Display star schema structure and summary
6. Execute analytic queries on star schema

Author: Axel Rosas
Date: 2026-04-10
"""

import sys
import os
from pathlib import Path
import sqlite3
from tabulate import tabulate
import pandas as pd
import traceback

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from create_database import create_database, DB_PATH as DB_TRANSACTIONAL
from load_sample_data import load_json_data
from pipeline_transactional_to_star import TransactionalToStarPipeline
from db_interactions import (
    get_connection,
    get_star_schema_summary,
    get_sales_by_cliente,
    get_revenue_by_month
)
from populate_database_pandas import populate_database


def print_header(title: str, width: int = 100):
    """Print a formatted section header."""
    print("\n" + "=" * width)
    print(f" {title}".ljust(width - 1))
    print("=" * width)


def print_subheader(title: str, width: int = 100):
    """Print a formatted subheader."""
    print("\n" + "-" * width)
    print(f" {title}")
    print("-" * width + "\n")


def display_transactional_schema():
    """Display the transactional database schema structure."""
    print_subheader("TRANSACTIONAL DATABASE SCHEMA - TABLE STRUCTURES")
    
    conn = get_connection("transactional")
    cursor = conn.cursor()
    
    # Get all tables in transactional database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table_row in tables:
        table_name = table_row[0]
        
        # Get table info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"\n📋 Table: {table_name}")
        print("-" * 80)
        
        # Prepare column info
        column_data = []
        for col_id, col_name, col_type, notnull, dflt_value, pk in columns:
            pk_indicator = "🔑 PK" if pk else ""
            null_indicator = "NOT NULL" if notnull else "NULL"
            column_data.append([col_name, col_type, null_indicator, pk_indicator])
        
        print(tabulate(
            column_data,
            headers=["Column", "Type", "Nullable", "Key"],
            tablefmt="grid"
        ))
    
    conn.close()


def display_star_schema():
    """Display the star schema database structure."""
    print_subheader("STAR SCHEMA DATABASE - TABLE STRUCTURES")
    
    conn = get_connection("star_schema")
    cursor = conn.cursor()
    
    # Get all tables in star schema database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table_row in tables:
        table_name = table_row[0]
        
        # Get table info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"\n📊 Table: {table_name}")
        print("-" * 80)
        
        # Prepare column info
        column_data = []
        for col_id, col_name, col_type, notnull, dflt_value, pk in columns:
            pk_indicator = "🔑 PK" if pk else ""
            null_indicator = "NOT NULL" if notnull else "NULL"
            column_data.append([col_name, col_type, null_indicator, pk_indicator])
        
        print(tabulate(
            column_data,
            headers=["Column", "Type", "Nullable", "Key"],
            tablefmt="grid"
        ))
    
    conn.close()


def display_star_schema_summary():
    """Display data summary for star schema."""
    print_subheader("STAR SCHEMA - DATA SUMMARY")
    
    summary = get_star_schema_summary()
    
    summary_data = [[table, count] for table, count in summary.items()]
    
    print(tabulate(
        summary_data,
        headers=["Table", "Record Count"],
        tablefmt="grid",
        numalign="right"
    ))
    
    print(f"\n✓ Total records across all dimension and fact tables: {sum(summary.values())}")


def execute_analytic_query_1():
    """
    Analytic Query 1: Sales by Customer
    Shows top customers by total sales volume
    """
    print_subheader("ANALYTIC QUERY 1: TOP CUSTOMERS BY SALES VOLUME")
    
    print("📈 This query shows the top customers ranked by total sales volume.")
    print("Metrics: Number of orders, Total sales, Average sale amount\n")
    
    try:
        df_sales = get_sales_by_cliente(limit=10)
        
        # Format numeric columns
        df_display = df_sales.copy()
        df_display['total_ventas'] = df_display['total_ventas'].apply(lambda x: f"${x:,.2f}")
        df_display['promedio_venta'] = df_display['promedio_venta'].apply(lambda x: f"${x:,.2f}")
        
        
        print(f"\n✓ Query executed successfully. Showing top {len(df_sales)} customers.\n")
        
    except Exception as e:
        print(f"❌ Error executing query 1: {e}\n")


def execute_analytic_query_2():
    """
    Analytic Query 2: Revenue by Month
    Shows monthly revenue trends
    """
    print_subheader("ANALYTIC QUERY 2: MONTHLY REVENUE TRENDS")
    
    print("📊 This query shows revenue trends broken down by year and month.")
    print("Metrics: Number of orders, Number of items sold, Total revenue, Average sale\n")
    
    try:
        df_revenue = get_revenue_by_month()
        
        # Format numeric columns
        df_display = df_revenue.copy()
        df_display['total_ingresos'] = df_display['total_ingresos'].apply(lambda x: f"${x:,.2f}")
        df_display['promedio_venta'] = df_display['promedio_venta'].apply(lambda x: f"${x:,.2f}")
        
        
        print(f"\n✓ Query executed successfully. Showing {len(df_revenue)} months of data.\n")
        
    except Exception as e:
        print(f"❌ Error executing query 2: {e}\n")


def main():
    """Main orchestration function."""
    print_header("DATABASE SETUP & STAR SCHEMA ANALYTICS WORKFLOW")
    
    try:
        # Step 1: Create Database
        print_subheader("STEP 1: CREATE TRANSACTIONAL DATABASE")
        print("Creating transactional database and tables...")
        create_database()
        print("✓ Transactional database created successfully\n")
        
        # Step 2: Display Transactional Schema
        print_header("STEP 2: TRANSACTIONAL DATABASE SCHEMA")
        display_transactional_schema()
        
        # Step 3: Load Sample Data
        print_subheader("STEP 3: LOAD SAMPLE DATA")
        print("Populating transactional database with sample data...")
        try:
            load_json_data()
            print("----------------Now using pandas to populate order details and update totals----------------")
            populate_database()
            print("✓ Sample data loaded successfully\n")
        except Exception as e:
            print(f"⚠ Sample data loading completed (some data may already exist): {e}\n")
        
        # Step 4: Run ETL Pipeline
        print_header("STEP 4: ETL PIPELINE - TRANSACTIONAL TO STAR SCHEMA")
        print("Running ETL pipeline to create and populate star schema...")
        print("This will:")
        print("  • Extract data from transactional database")
        print("  • Transform to star schema format (dimensions + fact table)")
        print("  • Load into star_schema.sqlite\n")
        
        pipeline = TransactionalToStarPipeline()
        pipeline.run_pipeline(clear_existing=True)
        print("✓ ETL pipeline completed successfully\n")
        
        # Step 5: Display Star Schema
        print_header("STEP 5: STAR SCHEMA DATABASE STRUCTURE")
        display_star_schema()
        
        # Step 6: Display Star Schema Summary
        print_header("STEP 6: STAR SCHEMA DATA SUMMARY")
        display_star_schema_summary()
        
        # Step 7: Execute Analytic Queries
        print_header("STEP 7: ANALYTIC QUERIES ON STAR SCHEMA")
        execute_analytic_query_1()
        execute_analytic_query_2()
        
        # Final summary
        print_header("✅ WORKFLOW COMPLETED SUCCESSFULLY")
        print("\n📁 Database Files Location:")
        print(f"   Transactional DB: {DB_TRANSACTIONAL}")
        print(f"   Star Schema DB:   {DB_TRANSACTIONAL.parent / 'star_schema.sqlite'}")
        print("\n💡 Next Steps:")
        print("   • Use db_interactions.py for custom queries")
        print("   • Use examples.py to see more usage patterns")
        print("   • Used query_database.py for interactive database exploration")
        print("\n" + "=" * 100 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
