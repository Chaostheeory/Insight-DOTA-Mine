import pandas as pd
import psycopg2
#from sqlalchemy import create_engine

psql_credeintal = {
        'database': 'wode',
        'user': 'wode',
        'password': '***',
        'host': '54.242.73.153',
        'port': '5432'
    }
con = psycopg2.connect(**psql_credeintal)

def get_winrate(user_id):
    query = "SELECT position, winrate FROM positions WHERE user_id='%s' order by position" % user_id
    query_results = pd.read_sql_query(query,con)
    return query_results


get_winrate(119807644)
