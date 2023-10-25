import csv
import psycopg2
from psycopg2 import sql

# Define the SQL CREATE TABLE statements for each table
create_accounts_table_sql = """
CREATE TABLE IF NOT EXISTS accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code INT,
    join_date DATE
);"""


create_products_table_sql = """
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_code INT,
    product_description VARCHAR(255)
);"""


create_transactions_table_sql = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(255),
    transaction_date DATE,
    product_id INT,
    product_code INT,
    product_description VARCHAR(255),
    quantity INT,
    account_id INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
);
"""


def get_column_names(csv_file):
    with open(csv_file, "r", newline="") as csvfile:
        csv_reader = csv.reader(csvfile)
        # Read the first row to obtain the column names
        column_names = next(csv_reader)
    return column_names


# Define your CREATE TABLE statement as you did before


def ingest_data(cursor, table_name, csv_file):
    with open(csv_file, "r", newline="") as csvfile:
        csv_reader = csv.reader(csvfile)
        # Read the first row to skip the header
        next(csv_reader)
        for row in csv_reader:
            insert_statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(
                    map(
                        lambda col: sql.Identifier(col.strip()),
                        get_column_names(csv_file),
                    )
                ),
                sql.SQL(", ").join([sql.Placeholder()] * len(row)),
            )
            cursor.execute(insert_statement, row)


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        cursor = conn.cursor()

        # Execute the CREATE TABLE statements
        cursor.execute(create_accounts_table_sql)
        cursor.execute(create_products_table_sql)
        cursor.execute(create_transactions_table_sql)

        # Ingest data from the CSV files

        ingest_data(cursor, "accounts", "/app/data/accounts.csv")
        ingest_data(cursor, "products", "/app/data/products.csv")
        ingest_data(cursor, "transactions", "/app/data/transactions.csv")

        # Commit changes
        conn.commit()
        print("Data ingested successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()
