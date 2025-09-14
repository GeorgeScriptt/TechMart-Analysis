import sqlite3
import pandas as pd
import os

# function to connect to a sqlite database and create a table
def setup_database(db_name="techmart.db"):
    """
     Connects to a SQLite database, creates tables if they don't exist,
    and populates them with data from CSV files.

    Args:
    db_name (str, optional): Name of the SQLite database file to create or connect to. Defaults to "techmart.db".

    Returns:
    None
    """

    conn = sqlite3.connect(db_name)  # connect to the database
    cur = conn.cursor()

    # Create Employee Records table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Employee_Records (
            employee_id INTEGER PRIMARY KEY,
            role TEXT,
            store_location TEXT,
            sales_performance REAL
        )
    """)

    # Create Product Details table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Product_Details (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            price REAL,
            stock INTEGER
        )
    """)

    # Create Customer Demographics table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Customer_Demographics (
            customer_id INTEGER PRIMARY KEY,
            age INTEGER,
            gender TEXT,
            location TEXT,
            loyalty_program TEXT
        )
    """)

    # Create Sales Transactions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Sales_Transactions (
            transaction_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            employee_id INTEGER,
            quantity INTEGER,
            total_amount REAL,
            sale_date TEXT,
            FOREIGN KEY (customer_id) REFERENCES Customer_Demographics(customer_id),
            FOREIGN KEY (product_id) REFERENCES Product_Details(product_id),
            FOREIGN KEY (employee_id) REFERENCES Employee_Records(employee_id)
        )
    """)

    conn.commit()

    # Load CSVs safely
    csv_files = {
        "Employee_Records": "Employee_Records.csv",
        "Product_Details": "Product_Details.csv",
        "Customer_Demographics": "Customer_Demographics.csv",
        "Sales_Transactions": "Sales_Transactions.csv"
    }

    # Clear existing data to avoid duplicates
    for table in csv_files.keys():
        cur.execute(f"DELETE FROM {table};")

    # Insert data
    for table, file in csv_files.items():
        if os.path.exists(file):
            df = pd.read_csv(file)
            df.to_sql(table, conn, if_exists="append", index=False)
        else:
            print(f"CSV file not found: {file}")

    print("Database setup complete: Tables created and populated with data!")


    conn.close()   # close database connection

    