from delta.tables import *
from pyspark.sql import DataFrame
import sys
from pyspark.dbutils import *

class Write:
    def __init__(self, spark, sdf):
        self.spark = spark
        self.sdf = sdf


    def write_sdf(self, base:dict, pk_column:list=['txhash','log_index'], part_column:list=['dt_utc'],min_date: str = '2022-01-01') -> None:
        try:
            self.spark.table(f"{base['Catalog']}.{base['Schema']}.{base['Table']}") 
            is_first = False
        except:
            is_first = True 

        try:
            if not is_first:
                self.merge_sdf(base['Catalog'], base['Schema'],base['Table'],pk_column,min_date)
                print("merge dataframe")
                
            else:
                self.overwrite_sdf(base['Catalog'], base['Schema'],base['Table'],pk_column,part_column)
                print("overwrite dataframe")
        except Exception as e:
            print(f"Error: {e}")
            raise Exception("Check Schema or Unique Key") 
    
    def overwrite_sdf(self, Catalog:str, Schema:str, Table:str, pk_column:list=['txhash','log_index'], part_column:list=['dt_utc']) -> None:
        pk_condition = " , ".join(
        [f"{col}" for col in pk_column])

        partition_condition = " , ".join(
        [f"{col}" for col in part_column])

        self.sdf.createOrReplaceTempView("tmp_view") 
        createDbSql = f"""
            CREATE TABLE IF NOT EXISTS {Catalog}.{Schema}.{Table}
            USING DELTA
            OPTIONS (
                primaryKey '{pk_condition}'
            )
            PARTITIONED BY ({partition_condition})
            AS
            select * from tmp_view WHERE 1 = 0
            """
        print(createDbSql)
        self.spark.sql(createDbSql)  # execute a sql

        self.spark.sql(
        f"""
        INSERT OVERWRITE TABLE {Catalog}.{Schema}.{Table}
        SELECT * FROM tmp_view
        """
    )
    def overwrite_sdf_no_partion_key(self, base, pk_column:list=['txhash','log_index']) -> None:
        pk_condition = " , ".join(
        [f"{col}" for col in pk_column])
        Catalog,Schema,Table = base['Catalog'], base['Schema'],base['Table']
        # partition_condition = " , ".join(
        # [f"{col}" for col in part_column])
        self.sdf.createOrReplaceTempView("tmp_view") 
        createDbSql = f"""
            CREATE TABLE IF NOT EXISTS {Catalog}.{Schema}.{Table}
            USING DELTA
            OPTIONS (
                primaryKey '{pk_condition}'
            )
            AS
            select * from tmp_view WHERE 1 = 0
            """
        print(createDbSql)
        self.spark.sql(createDbSql)  # execute a sql

        self.spark.sql(
        f"""
        INSERT OVERWRITE TABLE {Catalog}.{Schema}.{Table}
        SELECT * FROM tmp_view
        """
    )

    def merge_sdf(self,Catalog:str, Schema: str, Table: str, pk_column: list=['txhash','log_index'], min_date: str = '2022-01-01') -> None:
        if self.sdf.columns != pk_column :
            update_conditions = ", ".join(
            [f"target.`{col}` = source.`{col}`" for col in self.sdf.columns if col not in pk_column])
        else :
            update_conditions = ", ".join(
            [f"target.`{col}` = source.`{col}`" for col in self.sdf.columns])

        pk_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in pk_column])

        self.sdf.createOrReplaceTempView("tmp_view")
        # merge
        self.spark.sql(
            f"""
                MERGE INTO {Catalog}.{Schema}.{Table} AS target
                USING (SELECT * FROM tmp_view) AS source   -- update/insert 기준 날짜 filter 
                ON {pk_condition}  -- 기준 pk
                WHEN MATCHED THEN
                    UPDATE SET {update_conditions}
                WHEN NOT MATCHED THEN
                    INSERT *
            """
        )
      
