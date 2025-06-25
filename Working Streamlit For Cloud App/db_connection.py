##          This is a function to connect to the Neon Database platform where       ##
##              the information is stored in a Postgre SQL Database                 ##
##                                                                                  ##
##                              By Anthony Schauer                                  ##
######################################################################################
######################################################################################

import psycopg2
import streamlit as st

def connect_to_sql():
    db_url = st.secrets["DB_URL"]
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        return None
