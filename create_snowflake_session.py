from snowflake.snowpark import Session
import os

from dotenv import load_dotenv
load_dotenv()     # loads keys into os.environ so the rest of your code sees them

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

# Function call
print("Creating Session...")    
session = create_session_object()

# always close a session
print("Closing Session...")    
session.close()

