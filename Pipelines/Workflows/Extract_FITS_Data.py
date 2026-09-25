# Databricks notebook source

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')


from pyspark.sql.types import StructType, StructField, LongType, BinaryType
from delta.tables import DeltaTable

fits_data_table_name = f'{cat}.{db}.fits_files_data'

schema = StructType([StructField('imageid', LongType(), True), StructField('fits_data', BinaryType(), True)])
fits_data_tbl = DeltaTable.createIfNotExists(spark).tableName(fits_data_table_name).addColumns(schema).execute()


from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

@pandas_udf('binary')
def extract_fdata(fitscontent: pd.Series) -> pd.Series:
  from astropy.io import fits
  data_list = []
  for fc in fitscontent:
    cards = {}
    hdu = fits.PrimaryHDU.fromstring(bytes(fc), ignore_missing_end=False)
    data = hdu.data
    data_list.append(data.tobytes())
  return pd.Series(data_list)
  
spark.udf.register(name='extract_fdata',f=extract_fdata)


cmd = f'insert into {fits_data_table_name} select imageid, extract_fdata(content) as fits_data from {cat}.{db}.fits_files_raw where imageid not in (select imageid from {fits_data_table_name})'
spark.sql(cmd)
