import psycopg2
from psycopg2 import sql

def connect_postgres():
    try:
        connection = psycopg2.connect(
            host="db.wqyjmvwsippsgndjlvpk.supabase.co",   
            port="5432",
            database="postgres",          
            user="postgres",              
            password="Noronhja2004"      
        )

        print("Conexao realizada postgre")
        return connection

    except Exception as e:
        print("Erro:", e)
        return None
