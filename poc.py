from hdbcli import dbapi

# Start of the script
print("Starting the connection...")

# Connection details
conn = dbapi.connect(
    address='47ea62e3-34fc-490d-9f29-1395a0a7d833.hna0.prod-eu20.hanacloud.ondemand.com',  # Your HANA host
    port=443,  # Default port for HANA Cloud
    user='DBADMIN',  # Your HANA Cloud username
    password='sbxHANA1007'  # Your HANA Cloud password
)

print("Connected to HANA Cloud!")

# Create a cursor to interact with the database
cursor = conn.cursor()

# Simple INSERT statement with a few columns and example values
insert_statement = """
INSERT INTO "PRIMA_AI"."PRIMA_PRICE_DELTA_PYTHON_POC" (
    ABGANG_FIX, ABGANG_VAR, ABHOL_BZ_EINLIEFERUNG, ABHOL_FILIALE_EINLIEFERUNG
) 
VALUES (
    100.0, 200.0, 300.0, 400.0
);
"""

print("Executing INSERT statement...")

# Execute the INSERT statement and handle errors
try:
    cursor.execute(insert_statement)
    conn.commit()  # Commit the transaction
    print("Record inserted successfully!")
except dbapi.Error as e:
    print(f"Error: {e}")

# Close the connection
cursor.close()
conn.close()

print("Connection closed.")
