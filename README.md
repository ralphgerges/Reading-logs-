import json
import psycopg2
import os

def read_log_file(file_path):
    """Reads a .log file and extracts JSON data."""
    data_entries = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                data_entries.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line.strip()}")
    return data_entries

def insert_into_db(data, db_config):
    """Inserts extracted data into a PostgreSQL database."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO logs (timestamp, user_id, action, details)
        VALUES (%s, %s, %s, %s)
        """
        
        for entry in data:
            cursor.execute(insert_query, (
                entry.get('timestamp'), 
                entry.get('user_id'), 
                entry.get('action'), 
                json.dumps(entry.get('details'))
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Database error: {e}")

def main():
    """Main function to read log files and insert data into the database."""
    log_file_path = "data/logs.log"  # Change this to your actual log file path
    db_config = {
        "dbname": "your_db",
        "user": "your_user",
        "password": "your_password",
        "host": "localhost",
        "port": "5432"
    }
    
    if os.path.exists(log_file_path):
        log_data = read_log_file(log_file_path)
        if log_data:
            insert_into_db(log_data, db_config)
        else:
            print("No valid data found in the log file.")
    else:
        print("Log file not found.")

if __name__ == "__main__":
    main()
