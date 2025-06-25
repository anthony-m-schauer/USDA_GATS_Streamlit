##          This is a function to connect to the Neon Database platform where       ##
##              the information is stored in a Postgre SQL Database                 ##
##                                                                                  ##
##                              By Anthony Schauer                                  ##
######################################################################################
######################################################################################

import psycopg2

def connect_to_sql():
    try:
        conn = psycopg2.connect(
            dbname="neondb",
            user="neondb_owner",
            password="npg_Spg3yx4BsIRr",
            host="ep-dawn-violet-a84hks0v-pooler.eastus2.azure.neon.tech",
            sslmode="require"
        )
        return conn
    except Exception as e:
        return None