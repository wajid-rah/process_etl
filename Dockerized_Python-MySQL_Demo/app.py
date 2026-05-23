import mysql.connector
import time

print("Waiting for MySQL container...")

connection = None

# Retry loop until MySQL becomes ready
while connection is None:

    try:
        connection = mysql.connector.connect(
            host="mysql_container",
            user="root",
            password="root",
            database="testdb"
        )

    except Exception as e:
        print("MySQL not ready yet...")
        print("Retrying in 5 seconds...\n")
        time.sleep(5)

print("Connected successfully!\n")

# Create cursor object
cursor = connection.cursor()

# Execute SQL query
query = "SELECT * FROM employees"

print(f"Executing Query: {query}\n")

cursor.execute(query)

# Fetch all rows
rows = cursor.fetchall()

print("Employees Table Data")
print("-" * 30)

for row in rows:
    print(f"ID: {row[0]} | Name: {row[1]}")

print("-" * 30)

# Close resources
cursor.close()
connection.close()

print("\nConnection closed.")