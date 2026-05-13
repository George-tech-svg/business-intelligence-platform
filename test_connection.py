"""
TEST CONNECTION - Check if MySQL database connection works
Run this script to verify everything is configured correctly
"""

import mysql.connector
from mysql.connector import Error
import sys

print('-'*140)
print("TESTING MYSQL CONNECTION WITH ANALYST USER")
print('-'*140)

# Connection settings using analyst user
db_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'analyst',
    'password': 'analyst123',
    'database': 'business_intelligence'
}

print("Connection settings being used:")
print(f"  Host: {db_config['host']}")
print(f"  User: {db_config['user']}")
print(f"  Database: {db_config['database']}")
print('-'*140)

try:
    # Attempt to establish connection to MySQL
    connection = mysql.connector.connect(**db_config)
    
    if connection.is_connected():
        print("SUCCESS: Connected to MySQL database")
        
        # Get database server information
        db_info = connection.get_server_info()
        print(f"MySQL Server Version: {db_info}")
        
        # Get current user
        cursor = connection.cursor()
        cursor.execute("SELECT USER();")
        current_user = cursor.fetchone()
        print(f"Connected as user: {current_user[0]}")
        
        # Get current database
        cursor.execute("SELECT DATABASE();")
        current_db = cursor.fetchone()
        print(f"Using database: {current_db[0]}")
        
        # Test if we can create a table (analyst should be able to)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_connection (
                id INT AUTO_INCREMENT PRIMARY KEY,
                test_message VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("SUCCESS: Able to create tables in database")
        
        # Test if we can insert data
        cursor.execute("INSERT INTO test_connection (test_message) VALUES ('Connection test successful')")
        connection.commit()
        print("SUCCESS: Able to insert data into tables")
        
        # Test if we can read data
        cursor.execute("SELECT test_message, created_at FROM test_connection")
        result = cursor.fetchone()
        print(f"SUCCESS: Able to read data - Message: {result[0]}, Time: {result[1]}")
        
        # Clean up test table
        cursor.execute("DROP TABLE test_connection")
        connection.commit()
        print("SUCCESS: Able to delete tables (within your database)")
        
        cursor.close()
        connection.close()
        
        print('-'*140)
        print("ALL TESTS PASSED - Your MySQL connection is working correctly")
        print("The analyst user has proper permissions for this project")
        print('-'*140)
        
except Error as e:
    print(f"ERROR: MySQL connection failed")
    print(f"Error details: {e}")
    print('-'*140)
    print("Possible issues:")
    print("1. MySQL service is not running")
    print("2. Wrong password in config.py")
    print("3. Analyst user was not created correctly")
    print("4. Database name is incorrect")
    print('-'*140)
    sys.exit(1)

print('-'*140)
print("TEST COMPLETE")
print('-'*140)