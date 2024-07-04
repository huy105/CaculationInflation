from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from ELT import ETLSpark
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from dateutil.relativedelta import relativedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import configparser

app = FastAPI()


config = configparser.ConfigParser()
config.read('E:/Data/AI_Files/Spark/infor.ini')
password = config['mongodb_env_var']['password']

uri = f"mongodb+srv://giahuy105:{password}@cluster0.zsry683.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["InflationDB"]
collection = db["InflationIndex"]


def process_schema(column_names):

    schema = StructType([
        StructField(column_names[0], StringType(), True),
        StructField(column_names[1], StringType(), True),
        StructField(column_names[2], StringType(), True),
        StructField(column_names[3], FloatType(), True), 
    ])

    return schema

def get_data_from_past(spark_obj, schema: StructType, the_pass: int = 0):
    path = 'E:/Data/AI_Files/Spark/crawl_food/raw_data/'
    today = datetime.now()

    previous = today - relativedelta(day = the_pass)
    pre_day = previous.day
    pre_mon = previous.month
    pre_year = previous.year
    try:
        data = spark_obj.read.csv(path + f'{pre_day}-{pre_mon}-{pre_year}-raw_data.csv', header= True, schema = schema)
    except:
        data = None
    
    return data

def cal_index(spark: ETLSpark, schema):
    spark_obj = spark.return_spark_obj()
    data_now = get_data_from_past(spark_obj, schema, 0)
    
    document = {
        "timestamp": datetime.now(),
        "data": {}
    }
    
    time_range = [15, 30, 60, 90, 120, 240, 360]
    for value in time_range:
        inflation_rate = None
        data_time_range = get_data_from_past(spark_obj, schema, value)
        
        if data_time_range != None:
            inflation_rate = spark.spark_tranform_cpi(data_time_range, data_now)
        
        document['data'].update({f'{value}_interval': inflation_rate})

    collection.insert_one(document)

    return {'mess': 'sucess'}

@app.post('/caculate_cpi/')
async def caculate_cpi():

    spark = ETLSpark('Caculate CPI')
    spark.set_config()

    column_names = ['sku', 'category', 'name', 'price']
    schema = process_schema(column_names)
    
    result = cal_index(spark, schema)

    return {'mess': 'sucess'}
        