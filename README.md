# Assignment-1 - Algorithmic Digital Marketing

Snowflake SQL Query Tool
This is a web-based tool developed using Streamlit, designed to interact with Snowflake database. It allows users to run pre-defined SQL queries with dynamic parameters and view the results directly on the web interface.

Features
Query Selection: Users can select the desired query to run from a dropdown menu.
Dynamic Parameters: Users can input or select parameters for the queries.
Query Execution: Execute SQL queries on Snowflake database and view the results.
Environment Variable Loading: Load Snowflake credentials securely from a .env file.
Dependencies
Streamlit
SQLAlchemy
pandas
python-dotenv

Setup
1.Install the required Python packages:
pip install streamlit sqlalchemy pandas python-dotenv
2.Create a .env file in the root directory and add the Snowflake credentials:
SNOWFLAKE_username=your_username
SNOWFLAKE_password=your_password
SNOWFLAKE_account_identifier=your_account_identifier
3.Run the Streamlit app:
streamlit run app.py

Usage
Run the Streamlit app, and navigate to the URL provided in the terminal.
From the dropdown menu, select the query you wish to run.
Enter or select the desired parameters in the sidebar.
Click "Run Query" to execute the query and view the results.
Queries
The queries are defined in a dictionary in the script, and are designed to run on the SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL schema. Each query has a set of default parameters which can be overridden by the user.

Code Structure
Streamlit UI: The UI is built using Streamlit, and provides an easy-to-use interface for selecting queries, setting parameters, and viewing query results.
SQLAlchemy Engine: The SQLAlchemy engine is used to manage connections to the Snowflake database.
Query Execution: The run_query function is defined to execute a SQL query using the SQLAlchemy engine, and display the results using Streamlit.

https://codelabs-preview.appspot.com/?file_id=1fiUmLfqJvp6O3t8BzGxvYOlq7eFSemU6ry0qd9czwAk#0
