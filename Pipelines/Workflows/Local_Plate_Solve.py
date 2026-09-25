# Databricks notebook source
from pyspark.sql.functions import col, desc
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType, FloatType
from delta.tables import *
import pandas as pd
import astrolib.platesolve as ps
from astropy.table import Table
import numpy as np

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')

plate_solve_table = f'{cat}.{db}.fits_files_plate_solutions'
fits_headers = f'{cat}.{db}.fits_files_header'
centroids_table = f'{cat}.{db}.fits_files_centroids'

images = spark.sql(f'select imageid, naxis1, naxis2, pixscale from {fits_headers} as h where not exists (select * from {plate_solve_table} as s where h.imageid = s.imageid and s.simple is true)').collect()
#image_solutions = []


for image in images:
  sources = None
  imageid = image[0]
  image_width = image[1]
  image_height = image[2]
  pixelscale = image[3]
  sources = spark.read.table(centroids_table).where(col('imageid') == imageid).orderBy(col('flux').desc()).toPandas()
  print(f'Image {imageid} has {len(sources)} sources')
  if type(sources) is pd.DataFrame:
    sources = Table.from_pandas(sources)
  h = ps.solve_from_source_list_api(sources['xcentroid', 'ycentroid', 'flux'], image_width, image_height, pixelscale - 0.02, pixelscale + 0.02)
  column_str = f'{imageid},' # For using SQL INSERT
  image_solution = [imageid]
  for c in h.header.items():
    if c[0] != 'COMMENT' and c[0] != 'HISTORY' and c[0] != 'DATE':
      if type(c[1]) is str:
        column_str += f"'{c[1]}',"
      else:
          column_str += f'{c[1]},'
      image_solution.append(c[1])

  sqlcmd = f'insert into {plate_solve_table} values ({column_str[:-1]})'
  print(sqlcmd) # for debugging
  spark.sql(sqlcmd)
  #image_solutions.append(image_solution)