import psycopg2
import os

# connection details : This establishes a connection to the PostgreSQL server.

conn = psycopg2.connect(
    #host="my_postgres", # Without docker-compose.yml
    host=os.getenv("DB_HOST", "postgres"), # With docker-compose.yml
    port=os.getenv("DB_PORT", "5432"),
    database=os.getenv("DB_NAME", "wajiddb"),
    user=os.getenv("DB_USER", "etluser"),
    password=os.getenv("DB_PASS", "etlpassword")
)

# Create a cursor to execute SQL statement
cursor = conn.cursor()

# read all employees - updated
cursor.execute("SELECT * FROM employees ORDER BY id;")

# This retrieves all query results into a Python list.
rows = cursor.fetchall()
# rows = [
#     (1, 'John', 'HR', 50000, '2022-01-10'),
#     (2, 'Alice', 'IT', 75000, '2021-03-15'),
#     (3, 'Bob', 'Finance', 65000, '2020-07-20')
# ]

# display results
print("=" * 55)
print(f"{'ID':<5} {'Name':<15} {'Department':<15} {'Salary':<10} {'Join Date'}")
print("=" * 55)
# output
# ===========================================================
# ID    Name            Department      Salary     Join Date
# ===========================================================

# {'ID':<5}
#       Display "ID"
#       Left-align (<)
#       Use 5 character width

for row in rows:
    # row = (1, 'John', 'HR', 50000, '2022-01-10') and unpack values
    id, name, dept, salary, join_date = row
    print(f"{id:<5} {name:<15} {dept:<15} {str(salary):<10} {join_date}")

print("=" * 55)
print(f"Total employees: {len(rows)}")

# Close cursor and connection
cursor.close() # -> closes query handler
conn.close()  #  -> disconnects from PostgreSQL
# Why close them?
#     Frees database resources.
#     Prevents connection leaks.
#     Good practice after finishing database operations.
#
