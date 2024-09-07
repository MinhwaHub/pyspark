from datetime import datetime, date, timedelta
from pyspark.dbutils import *
import sys
import re
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pymongo import MongoClient
import time

class Common:
    def __init__(self, spark):
        self.spark = spark
        
    def add_datetime(self, sdf : DataFrame, timestamp_col='time') -> DataFrame:
        final_sdf = sdf.withColumn(
                    "ts_utc",
                    to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss")
                ).withColumn(
                    "dt_utc",
                    to_date(to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss"), 'yyyy-MM-dd')
                ).withColumn(
                    "ts_kst",
                    from_utc_timestamp(to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss"), "Asia/Tokyo")
                ).withColumn(
                    "dt_kst",
                    to_date(from_utc_timestamp(to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss"), "Asia/Tokyo"), 'yyyy-MM-dd')
                ).withColumn(
                    "ts_hkt",
                    from_utc_timestamp(to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss"), "Asia/Shanghai")
                ).withColumn(
                    "dt_hkt",
                    to_date(from_utc_timestamp(to_timestamp(from_unixtime(timestamp_col), "yyyy-MM-dd HH:mm:ss"), "Asia/Shanghai"), 'yyyy-MM-dd')).drop('_timestamp')
        
        return final_sdf
    
    def column_to_snake(self, sdf:DataFrame) -> DataFrame:
        def camel_to_snake(column_name):
            result = [column_name[0].lower()]
            for char in column_name[1:]:
                if char.isupper():
                    result.extend(['_', char.lower()])
                else:
                    result.append(char)
            return ''.join(result)

        new_columns = [camel_to_snake(column) for column in sdf.columns]

        sdf = sdf.toDF(*new_columns)

        return sdf 
    
    def rename_cols(self, sdf : DataFrame, edit_cols:dict) -> DataFrame:
        for col in sdf.columns:
            if col in edit_cols.keys():
                sdf = sdf.withColumnRenamed(col, edit_cols[col])

            if any(c.isupper() for c in col): 
                new_col = '_'.join(col.split()).lower() 
                sdf = sdf.withColumnRenamed(col, new_col)

        return sdf

    def rename_val(self, sdf: DataFrame, target_col:str, edit_val:dict) -> DataFrame:
        for val in edit_val.keys():
            sdf = sdf.withColumn(target_col, when(col(f'{target_col}')==val, edit_val[val]).otherwise(col(target_col)))

        return sdf
    
    def flatten_json(self, sdf: DataFrame, target_col: str) -> DataFrame:
        key_list = (
            sdf.select(explode(map_keys(expr(f"from_json({target_col}, 'map<string,string>')"))).alias("key"))
            .distinct()
            .collect()
        )   
        key_list = [row.key for row in key_list]
        col_type = ", ".join([f"{row} string" for row in key_list])

        sdf1 = (
            sdf.withColumn(
                "parsed_data",
                expr(
                    f"from_json({target_col}, '{col_type}')"
                )
            ).drop("eventfunc")
        )

        select_list = [expr(f"parsed_data.{row} as {row}") for row in key_list]
        final_sdf = sdf1.select("*", *select_list).drop("parsed_data")

        return final_sdf
    
    def convert_hex(self, sdf:DataFrame, target_col:list) -> DataFrame:
        convert_hex_to_ascii = udf(lambda x: bytes.fromhex(re.sub("0x|00", "", x)).decode())
        for i in target_col:
            sdf = sdf.withColumn(f"{i}_str", convert_hex_to_ascii(i))
        
        return sdf 
    
    def to_ether(self, sdf:DataFrame, target_col:list) -> DataFrame:
        for i in target_col:
            i = i.lower()
            sdf = sdf.withColumn(f"{i}_ether", (col(i)/ pow(lit(10), lit(18))).cast("decimal(38,10)"))
        
        return sdf
    
    def get_max_bdate(self, base:dict) -> str:
        try :
            max_time = self.spark.sql(f"select max(bdate) as max_time from {base['catalog_name']}.{base['schema_name']}.{base['table_name']}").first().max_time
        except :
            max_time = 0
        return max_time


    def change_type(self, sdf: DataFrame, match_type:dict):
        for keys in match_type:
            if match_type[keys] == 'integer':
                sdf = sdf.withColumn(keys, col(keys).cast(IntegerType()))
            elif match_type[keys] == 'timestamp':
                sdf = sdf.withColumn(keys, col(keys).cast(TimestampType()))
            elif match_type[keys] == 'string':
                sdf = sdf.withColumn(keys, col(keys).cast(StringType()))    
            elif match_type[keys] == 'bool':
                sdf = sdf.withColumn(keys, col(keys).cast(BooleanType()))       

        return sdf
        
    def get_info_from_notebook(self) -> Union[str, bool]:
        data = {}
        if datetime.now().day != (datetime.now() + timedelta(hours=9)).day:
            start_date = date.today()
            ts_y = int(time.mktime(start_date.timetuple()) -32400)
        else:
            start_date = date.today() - timedelta(days=1)
            ts_y = int(time.mktime(start_date.timetuple()))
            
        year = start_date.year
        month = str(start_date.month).zfill(2)
        last_month = str((start_date - timedelta(days=30)).month).zfill(2)
        day = str(start_date.day).zfill(2)
        yesterday = str((start_date - timedelta(days=1)).day).zfill(2)

        dbutils = DBUtils(self.spark)
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

        if 'dw' in notebook_path:
            filename = notebook_path.split('dw/')[-1].lower()  #상위 폴더명
            sourceFolder = notebook_path.split('dw_')[-1].lower()
        if 'dl' in notebook_path:
            filename = notebook_path.split('dl/')[-1].lower()    
            sourceFolder = notebook_path.split('dl_')[-1].lower()
        if 'dm' in notebook_path:
            filename = notebook_path.split('dm/')[-1].lower()
            sourceFolder = notebook_path.split('dm_')[-1].lower()
            data['PK'] = ['timezone_type','bdate']


        bdate_format = "%Y-%m-%d"
        catalog_name = "catalog_sth"
        service_name = notebook_path.split('/')[-3]
        database_name = notebook_path.split('/')[-2]
        schema_name = service_name
        table_name = filename

        
        try:
            self.spark.table(f"{catalog_name}.{schema_name}.{table_name}")
            is_first =False
        except:
            is_first = True
            
        data['year'] = year
        data['month'] = month
        data['last_month'] = last_month
        data['day'] = day
        data['yesterday'] = yesterday
        data['ts_y'] = ts_y
        data['ts_t'] = ts_y + 86400
        data['notebook_path'] = notebook_path
        data['filename'] = filename
        data['sourceFolder'] = sourceFolder
        data['Catalog'] = catalog_name
        data['Schema'] = schema_name
        data['Table'] = table_name
        data['is_first'] = is_first

        return data
         