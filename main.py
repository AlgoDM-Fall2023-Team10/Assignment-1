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





