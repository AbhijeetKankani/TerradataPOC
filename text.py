from hdbcli import dbapi

# Function to read data from a text file
def read_data_from_file(file_path):
    data = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Strip and split each line by comma
                values = line.strip().split(',')
                # Convert each value to a float and add to the data list
                data.append([float(value) for value in values])
    except Exception as e:
        print(f"Error reading file: {e}")
    return data

# Function to insert data into HANA Cloud
def insert_data_to_hana(conn, data):
    cursor = conn.cursor()
    
    # Dynamic insert statement with placeholders
    insert_statement = """
    INSERT INTO "PRIMA_AI"."PRIMA_PRICE_DELTA_PYTHON_POC" (
        ABGANG_FIX, ABGANG_VAR, ABHOL_BZ_EINLIEFERUNG, ABHOL_FILIALE_EINLIEFERUNG
    ) 
    VALUES (?, ?, ?, ?);
    """
    
    # Execute the insert statement for each row of data
    try:
        for row in data:
            cursor.execute(insert_statement, row)
        conn.commit()  # Commit the transaction
        print(f"{len(data)} records inserted successfully!")
    except dbapi.Error as e:
        print(f"Error during insertion: {e}")
    finally:
        cursor.close()

# Main function to establish the connection and perform the insert operation
def main():
    print("Starting the connection...")

    # Connection details
    conn = dbapi.connect(
        address='47ea62e3-34fc-490d-9f29-1395a0a7d833.hna0.prod-eu20.hanacloud.ondemand.com',  # Your HANA host
        port=443,  # Default port for HANA Cloud
        user='DBADMIN',  # Your HANA Cloud username
        password='sbxHANA1007'  # Your HANA Cloud password
    )

    print("Connected to HANA Cloud!")

    # Read data from the file (update with your file path)
    file_path = r'C:\Users\akankani\OneDrive - DPDHL\Desktop\HANADB.txt'
    data = read_data_from_file(file_path)
    
    if data:
        # Insert data into HANA Cloud
        insert_data_to_hana(conn, data)
    else:
        print("No data to insert.")

    # Close the connection
    conn.close()
    print("Connection closed.")

# Run the script
if __name__ == "__main__":
    main()
