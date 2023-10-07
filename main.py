import streamlit as st
import sqlalchemy
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


# SQL queries
queries = {
    'Query 1': """
        SELECT 
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        SUM(cr_net_loss) Returns_Loss
        FROM
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.call_center,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.catalog_returns,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer_address,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer_demographics,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.household_demographics
        WHERE
        cr_call_center_sk = cc_call_center_sk
        AND cr_returned_date_sk = d_date_sk
        AND cr_returning_customer_sk = c_customer_sk
        AND cd_demo_sk = c_current_cdemo_sk
        AND hd_demo_sk = c_current_hdemo_sk
        AND ca_address_sk = c_current_addr_sk
        AND d_year = :d_year
        AND d_moy = :d_moy
        AND (
            (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
            OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
        )
        AND hd_buy_potential LIKE :hd_buy_potential
        AND ca_gmt_offset = :ca_gmt_offset
        GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
        ORDER BY SUM(cr_net_loss) DESC;
    """,
    'Query 2': """
        select 
        sum(ws_ext_discount_amt)  as "Excess Discount Amount"
        from
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.item,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        where
        i_manufact_id = :i_manufact_id
        and i_item_sk = ws_item_sk
        and d_date between :start_date and
        (cast(:start_date as date) + interval '90 days')
        and d_date_sk = ws_sold_date_sk
        and ws_ext_discount_amt
        > (
        SELECT
            1.3 * avg(ws_ext_discount_amt)
        FROM
             SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales,
             SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        WHERE
             ws_item_sk = i_item_sk
             and d_date between :start_date and
             (cast(:start_date as date) + interval '90 days')
             and d_date_sk = ws_sold_date_sk
        )
         order by sum(ws_ext_discount_amt)
         limit 100;
    """,
    'Query 3': """
        select  
        ss_customer_sk,  
        sum(act_sales) as sumsales
        from  
        (select  
        ss_item_sk,  
        ss_ticket_number,  
        ss_customer_sk,  
        case when sr_return_quantity is not null then (ss_quantity - sr_return_quantity) * ss_sales_price  
             else (ss_quantity * ss_sales_price) end as act_sales  
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store_sales  
        left outer join SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store_returns on (sr_item_sk = ss_item_sk  
                                         and sr_ticket_number = ss_ticket_number)  
        , SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.reason  
        where sr_reason_sk = r_reason_sk  
          and r_reason_desc = :r_reason_desc) t  
        group by ss_customer_sk  
        order by sumsales, ss_customer_sk  
        limit 100;

    """,
    'Query 4': """
        select  
        count(distinct ws_order_number) as "order count",
        sum(ws_ext_ship_cost) as "total shipping cost",
        sum(ws_net_profit) as "total net profit"
        from
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales ws1,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer_address,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_site
        where
        d_date between :start_date and 
        dateadd(day,60,to_date(:start_date))
        and ws1.ws_ship_date_sk = d_date_sk
        and ws1.ws_ship_addr_sk = ca_address_sk
        and ca_state = :ca_state
        and ws1.ws_web_site_sk = web_site_sk
        and web_company_name = 'pri'
        and exists (select *
            from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
                and not exists(select *
               from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
        order by count(distinct ws_order_number)
        limit 100;
    """,
    'Query 5': """
        with ws_wh as
        (select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales ws1,SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales ws2
        where ws1.ws_order_number = ws2.ws_order_number
        and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
        select  
        count(distinct ws_order_number) as "order count",
        sum(ws_ext_ship_cost) as "total shipping cost",
        sum(ws_net_profit) as "total net profit"
        from
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales ws1,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer_address,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_site
        where
        d_date between :start_date and 
        dateadd(day,60,to_date(:start_date))
        and ws1.ws_ship_date_sk = d_date_sk
        and ws1.ws_ship_addr_sk = ca_address_sk
        and ca_state = :ca_state
        and ws1.ws_web_site_sk = web_site_sk
        and web_company_name = 'pri'
        and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
        and ws1.ws_order_number in (select wr_order_number
                            from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
        order by count(distinct ws_order_number)
        limit 100;
    """,
    'Query 6': """
        select  count(*) 
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store_sales,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.household_demographics, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.time_dim, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store
        where ss_sold_time_sk = time_dim.t_time_sk   
        and ss_hdemo_sk = household_demographics.hd_demo_sk 
        and ss_store_sk = s_store_sk
        and time_dim.t_hour = :t_hour
        and time_dim.t_minute >= 30
        and household_demographics.hd_dep_count = :hd_dep_count
        and store.s_store_name = 'ese'
        order by count(*)
        limit 100;
    """,
    'Query 7': """
        with ssci as (
        select ss_customer_sk customer_sk,
        ss_item_sk item_sk
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store_sales,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        where ss_sold_date_sk = d_date_sk
         and d_month_seq between 1214 and 1214 + 11
            group by ss_customer_sk,
            ss_item_sk),
        csci as(
        select cs_bill_customer_sk customer_sk,
        cs_item_sk item_sk
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.catalog_sales,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        where cs_sold_date_sk = d_date_sk
        and d_month_seq between :YEAR01 and :YEAR01 + 11
        group by cs_bill_customer_sk,
        cs_item_sk)
        select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only,
        sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only,
        sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
        from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
        and ssci.item_sk = csci.item_sk)
        limit 100;
    """,
    'Query 8': """
        select i_item_id,
        i_item_desc, 
        i_category, 
        i_class, 
        i_current_price,
        sum(ss_ext_sales_price) as itemrevenue, 
        sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
        (partition by i_class) as revenueratio
        from	
	    SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.store_sales,
    	SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.item, 
    	SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        where 
	    ss_item_sk = i_item_sk 
  	    and i_category in (:CATEGORY01, :CATEGORY02, :CATEGORY03)
  	    and ss_sold_date_sk = d_date_sk
	    and d_date between cast(:start_date as date) 
		and dateadd(day,30,to_date(:start_date))
        group by 
	    i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
        order by 
	    i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;
    """,
    'Query 9': """
        select  
        substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
        ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
        ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
        ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
        ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
        ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
        from
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.catalog_sales,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.warehouse,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ship_mode,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.call_center,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
        where
        d_month_seq between :DMS01 and :DMS01 + 11
        and cs_ship_date_sk   = d_date_sk
        and cs_warehouse_sk   = w_warehouse_sk
        and cs_ship_mode_sk   = sm_ship_mode_sk
        and cs_call_center_sk = cc_call_center_sk
        group by
        substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
        order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
        limit 100;
    """,
    'Query 10': """
        select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
        from ( select count(*) amc
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.household_demographics , 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.time_dim, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_page
        where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between :HOUR_AM01 and :HOUR_AM01 +1
         and household_demographics.hd_dep_count = :DEPCNT01
         and web_page.wp_char_count between 5000 and 5200) at,
        ( select count(*) pmc
        from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.household_demographics, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.time_dim, 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_page
        where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between :HOUR_PM01 and :HOUR_PM01 +1
         and household_demographics.hd_dep_count = :DEPCNT01
         and web_page.wp_char_count between 5000 and 5200) pt
        order by am_pm_ratio
        limit 100;
    """,
}

# Display the SQL code for the selected query
st.subheader("SQL Query:")
st.text_area(label=" ", value=queries[selected_query], height=400)

# To display the output of selected query
if st.button("Run Query", key="run_query_button"):
    params = query_params[selected_query]
    run_query(selected_query, params)


