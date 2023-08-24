from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, regexp_replace, format_number
import time

class ETLSpark:

    def __init__(self, app_name: str = '') -> None:
        
        self.spark = SparkSession.builder \
            .appName(app_name) \

    def set_config(self) -> None:
        conf = SparkConf() \
            .setMaster("spark://192.168.1.6:7077") \
            .set("spark.executor.cores", "6") \
            .set("spark.executor.memory", "2g") \
            .set("spark.driver.memory", "4g")  

        self.spark = self.spark.config(conf = conf).getOrCreate()
        
    def spark_tranform_cpi(self, df_old: DataFrame, df_new: DataFrame):
        
        df_new = df_new.withColumnRenamed("price", "new_price")
        result = df_old.join(df_new, on=['sku', 'category', 'name'], how='inner') 
        
        cpi_new = result.select(col("new_price")).agg({"new_price": "sum"}).collect()[0][0]
        cpi_old = result.select(col("price")).agg({"price": "sum"}).collect()[0][0]
        inflation_rate  = ((cpi_new - cpi_old) / cpi_old) * 100
        
        return inflation_rate

    def return_spark_obj(self):
        return self.spark

    def stop(self):
        self.spark.stop()

# spark = ETLSpark('Testing spark')
# spark.set_config()
# spark.spark_read_source()
# spark.stop()