from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import sys, os

from dotenv import load_dotenv
load_dotenv()     # loads keys into os.environ so the rest of your code sees them

print(f"VS Code is using this Python interpreter: {sys.executable}")

# Create Session object
def create_session_object():
    # Define connection parameters
    connection_parameters = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "role": os.getenv('SNOWFLAKE_ROLE'),
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "database": os.getenv('SNOWFLAKE_DATABASE'),
        "schema": os.getenv('SNOWFLAKE_SCHEMA')
    }
    session = Session.builder.configs(connection_parameters).create()
    # Print the contents of your session object
    print(session)
    
    return session

def process_data(session_to_use):
    """This function reads and shows data from our SAMPLE_TABLE."""

    print("Starting data processing...")

    # Read the table into a DataFrame
    df_table = session_to_use.table("SAMPLE_TABLE")

    # Show the first 10 rows of the table
    print("Showing a sample of SAMPLE_TABLE:")
    df_table.show()

    # Create a new, filtered DataFrame
    df_filtered = df_table.filter(col("CS_BILL_CUSTOMER_SK") > 6000000)

    # Show first 10 rows of filtered table
    print("Showing a sample of the filtered data:")
    df_filtered.show()

    print("Data processing function finished")


print("---SCRIPT START ---")

# Create session
print("[Action 1] Connecting to Snowflake...")
session = create_session_object()

# Use the session to create our SAMPLE_TABLE
print("[Action 2] Creating the SAMPLE_TABLE")
source_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL_OLD.CATALOG_SALES").limit(1000)
source_df.write.mode("overwrite").save_as_table("sample_table")

print("SAMPLE_TABLE created successfully!")

# Call processing function to work with the new table
print("[Action 3] Calling the function to process our data...")
process_data(session)

# Close connection to Snowflake
print("[Action 4] Closing the session...")
session.close()
print("Session closed.")
