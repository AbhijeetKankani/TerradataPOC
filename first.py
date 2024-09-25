from hdbcli import dbapi

print("Starting the connection...")

# Connection details
conn = dbapi.connect(
    address='47ea62e3-34fc-490d-9f29-1395a0a7d833.hna0.prod-eu20.hanacloud.ondemand.com', 
    port=443,  
    user='DBADMIN',  
    password='sbxHANA1007'  
)

print("Connected to HANA Cloud!")

# Create a cursor to interact with the database
cursor = conn.cursor()

# INSERT statement
insert_statement = """
INSERT INTO "PRIMA_AI"."PRIMA_PRICE_DELTA_PYTHON_POC" VALUES
    (100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 'AB1', 'AR001', 'ARX001', 10, 20, 30, 'AGK01',
     50.0, 60.0, 40, 10.5, 25.5, 35.5, 45.5, 'YES', 15.0, 12.5, 10.0, 20.0, 18.0, 22.5, 'N', 
     24.5, 25.5, 'INFO', 'COMMENT', 'SAVINGS', 40.5, 32.3, 'X',
     ...);
"""

print("Executing INSERT statement...")

try:
    cursor.execute(insert_statement)
    conn.commit()  # Commit the transaction
    print("Record inserted successfully")
except dbapi.Error as e:
    print(f"Error: {e}")

# Close the connection
cursor.close()
conn.close()

print("Connection closed.")
