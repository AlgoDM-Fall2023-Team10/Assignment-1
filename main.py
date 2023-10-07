import streamlit as st
from sqlalchemy import create_engine, text, bindparam
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Streamlit UI
st.title("Snowflake SQL Query Tool")


# Load environment variables from .env
load_dotenv()

# Retrieve Snowflake credentials from environment variables
SNOWFLAKE_username = os.getenv("SNOWFLAKE_username")
SNOWFLAKE_password = os.getenv("SNOWFLAKE_password")
SNOWFLAKE_account_identifier = os.getenv("SNOWFLAKE_account_identifier")

# Create a Snowflake connection URL
snowflake_connection_url = f"snowflake://{SNOWFLAKE_username}:{SNOWFLAKE_password}@{SNOWFLAKE_account_identifier}"
snowflake_engine = create_engine(snowflake_connection_url)

# Dictionary to define default parameters
query_default_params = {
    'Query 1': {
        'd_year': [1998],          
        'd_moy': [11],            
        'hd_buy_potential': ['Unknown'],  
        'ca_gmt_offset': [-6],     
    },
    'Query 2':{
        'i_manufact_id': [350],
        'start_date': ['2000-01-27']
    },
    'Query 3':{
        'r_reason_desc': ["reason 70"]
    },
    'Query 4': {
        'ca_state':['CA'],
        'start_date':['2002-02-01']
    },
    'Query 5': {
        'ca_state':['TN'],
        'start_date':['2002-02-01']
     },
     'Query 6': {
        't_hour':[8],
        'hd_dep_count':[5]
     },
    'Query 7': {
        'YEAR01':['1214']
    },
    'Query 8': {
       'start_date':['2002-05-27'], 
       'CATEGORY01':['Women'],
       'CATEGORY02':['Electronics'],
       'CATEGORY03':['Shoes']
    },
    'Query 9': {
        'DMS01':[1200]
    },
    'Query 10': {
        'HOUR_PM01':[18],
        'HOUR_AM01':[8],
        'DEPCNT01':[9]
    },
    }
# Function to run the query with dynamic parameters
def run_query(selected_query, params):
    selected_sql_query = queries[selected_query]
    default_params = query_default_params[selected_query]  # Get default params for the selected query

    # Update the params dictionary with user-selected values or defaults
    for param_name, param_value in default_params.items():
        if param_name not in params:
            params[param_name] = param_value

    try:
        # Create a parameterized query with bindparams for all selected values
        param_query = text(selected_sql_query).bindparams(**params)

        # Execute the SQL query
        with snowflake_engine.connect() as connection:
            result = connection.execute(param_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Display the query result
        st.write("Query Result:")
        st.dataframe(df)

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        
# Dictionary to define substitution parameters
query_params = {
    'Query 1': {
        'd_year': [1998,2002],
        'd_moy': [11,12],
        'hd_buy_potential': ["0-500%",'Unknown'],
        'ca_gmt_offset': [-6,-7]
    },
    'Query 2': {
        'i_manufact_id': [605, 350],
        'start_date': ['2000-01-23', '2000-01-27']
    },
    'Query 3': {
        'r_reason_desc': ["reason 70","reason 28"]
    },
    'Query 4': {
        'ca_state':['CA','IL'],
        'start_date':['2002-02-01','1999-02-01']
    },
    'Query 5': {
        'ca_state':['TN','IL'],
        'start_date':['2002-02-01','1999-02-01']
     },
     'Query 6': {
        't_hour':[8,20],
        'hd_dep_count':[5,7]
     },
    'Query 7': {
        'YEAR01':['1214','2000']
    },
    'Query 8': {
       'start_date':['2002-05-27','1999-02-22'], 
       'CATEGORY01':['Women','Sports'],
       'CATEGORY02':['Electronics','Books'],
       'CATEGORY03':['Shoes','Home']
    },
    'Query 9': {
        'DMS01':[1200]
    },
    'Query 10': {
        'HOUR_PM01':[18,19],
        'HOUR_AM01':[8],
        'DEPCNT01':[9,6]
    },
}

# Create a dropdown to select the query
selected_query = st.selectbox("Select a Query:", list(query_params.keys()))

# Display the selected query
st.write(f"You selected: {selected_query}")

# Create a sidebar for parameter inputs
st.sidebar.title("Query Parameters")


# Display query-specific parameters in the sidebar
if selected_query in query_params:
    params = query_params[selected_query]
    default_params = query_default_params[selected_query]  # Get default params for the selected query
    for param_name, param_value in params.items():
        if isinstance(param_value, list):
            # Use default value as the initial selected value
            params[param_name] = st.sidebar.multiselect(f"Select {param_name}:", param_value, default=default_params.get(param_name, []))
        elif isinstance(param_value, str):
            # Use default value as the initial input field value
            params[param_name] = st.sidebar.text_input(f"Enter {param_name}:", value=default_params.get(param_name, param_value))
        else:
            st.sidebar.text(f"Unsupported parameter type for {param_name}")

    # Display the selected values
    st.sidebar.write("Selected Parameters:")
    for param_name, param_value in params.items():
        st.sidebar.write(f"{param_name}: {param_value}")




