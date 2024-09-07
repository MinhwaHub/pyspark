import psycopg2
from pyspark.sql import SparkSession
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.types import *
from databricks.sdk.runtime import *
import datetime
import urllib.parse
import pymysql
from pymongo import MongoClient


class RedshiftUtil:
    def __init__(self,spark):
        self.spark = spark
        self.secret_value = os.environ.get('secret_value')


    def redshift_to_sparkdf(self, sql_query):
        connection = None
        results = None

        variables = {}

        HOST = self.secret_value.split(';')[0]
        PORT = self.secret_value.split(';')[1]
        DBNAME = self.secret_value.split(';')[2]
        USER = self.secret_value.split(';')[3]
        PASSWORD = self.secret_value.split(';')[4]

        try:
            connection = psycopg2.connect(
                host=HOST,
                port=PORT,
                dbname=DBNAME,
                user=USER,
                password=PASSWORD
            )

            cursor = connection.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

        except Exception as e:
            print("Error while connecting to Redshift and executing the query:", e)

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Redshift connection closed.")

        if results is not None:
            new_data = [
                [
                    'NULL' if x is None else str(x) if isinstance(x, datetime.datetime) else x
                    for x in item
                ]
                for item in results
            ]
        else:
            new_data = []

        spark = SparkSession.builder.appName("RedshiftQuery").getOrCreate()
        sdf = self.spark.createDataFrame(new_data, schema=column_names)
        return sdf
  
class MySQLUtil:
    def __init__(self, spark, key_name):
        self.spark = spark
        self.key_name = key_name
    
    def db_to_sparkdf(self, sql_query, scope_name="scope_akv_all"):
        jdbc_conn = dbutils.secrets.get(scope=scope_name, key=self.key_name)
        db_host = jdbc_conn.split("Server=")[1].split(';')[0]
        db_user = 'bi_rouser'
        db_pwd = jdbc_conn.split("Password=")[1].split(';Database')[0]
        database = jdbc_conn.split("Database=")[1]

        with Connection(host=db_host, user=db_user, password=db_pwd) as connection:
            cursor = connection.cursor()
            cursor.execute(f"USE {database}")   
            cursor.execute(f"{sql_query}")
            
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            cursor.close()
        
        cleaned_results = []
        for row in results:
            cleaned_row = [value if value is not None else '' for value in row]  # 예: 빈 문자열로 대체
            cleaned_results.append(cleaned_row)

        if len(cleaned_results) >0:
            df = self.spark.createDataFrame(cleaned_results, columns)
        else:
            df = False
        
        return df
    
class MongoDBUtil:
    def __init__(self, spark, key_name):
        self.spark = spark
        self.key_name = key_name
    
    def db_to_sparkdf(self, collection, base_time:str,time_gte:int, time_lt:int, select_field:dict={}, match_field:dict={}, scope_name="scope_akv_all"):
        jdbc_conn = dbutils.secrets.get(scope=scope_name, key=self.key_name)
        db_host = jdbc_conn.split("Server=")[1].split(';')[0]
        db_user = 'bi_rouser'
        db_pwd = jdbc_conn.split("Password=")[1].split(';Database')[0]
        database = jdbc_conn.split("Database=")[1]

        client = MongoClient(db_host)
        db = client[database]
        query = match_field
        query[base_time] = {'$gte':time_gte, '$lt':time_lt}
        result = list(db[collection].find(query,select_field))

        cleaned_data = []
        json_schema = MapType(StringType(), StringType())

        for item in result:
            cleaned_item = {key: str(value) for key, value in item.items()}
            cleaned_data.append(cleaned_item)
        sdf= self.spark.createDataFrame(cleaned_data)

        return sdf
