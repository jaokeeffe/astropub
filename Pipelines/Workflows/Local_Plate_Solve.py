# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists `${cat}`.`${db}`.fits_files_plate_solutions
# MAGIC (imageid long,
# MAGIC simple boolean,
# MAGIC bitpix int,
# MAGIC naxis int,
# MAGIC extend boolean,
# MAGIC wcsaxes int,
# MAGIC ctype1 string,
# MAGIC ctype2 string,
# MAGIC equinox float,
# MAGIC lonpole float,
# MAGIC latpole float,
# MAGIC crval1 float,
# MAGIC crval2 float,
# MAGIC crpix1 float,
# MAGIC crpix2 float,
# MAGIC cunit1 string,
# MAGIC cunit2 string,
# MAGIC cd1_1 float,
# MAGIC cd1_2 float,
# MAGIC cd2_1 float,
# MAGIC cd2_2 float,
# MAGIC imagew int,
# MAGIC imageh int,
# MAGIC a_order int,
# MAGIC a_0_0 float,
# MAGIC a_0_1 float,
# MAGIC a_0_2 float,
# MAGIC a_1_0 float,
# MAGIC a_1_1 float,
# MAGIC a_2_0 float,
# MAGIC b_order int,
# MAGIC b_0_0 float,
# MAGIC b_0_1 float,
# MAGIC b_0_2 float,
# MAGIC b_1_0 float,
# MAGIC b_1_1 float,
# MAGIC b_2_0 float,
# MAGIC ap_order int,
# MAGIC ap_0_0 float,
# MAGIC ap_0_1 float,
# MAGIC ap_0_2 float,
# MAGIC ap_1_0 float,
# MAGIC ap_1_1 float,
# MAGIC ap_2_0 float,
# MAGIC bp_order int,
# MAGIC bp_0_0 float,
# MAGIC bp_0_1 float,
# MAGIC bp_0_2 float,
# MAGIC bp_1_0 float,
# MAGIC bp_1_1 float,
# MAGIC bp_2_0 float
# MAGIC )

# COMMAND ----------

from pyspark.sql.functions import col, desc
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType, FloatType
from delta.tables import *
import pandas as pd
import astrolib.platesolve as ps
from astropy.table import Table
import numpy as np

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')
ps_url = dbutils.widgets.get('ps_url')

plate_solve_table = f'`{cat}`.`{db}`.fits_files_plate_solutions'
fits_headers = f'`{cat}`.`{db}`.fits_files_header'
centroids_table = f'`{cat}`.`{db}`.fits_files_centroids'

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
  h = ps.solve_from_source_list_api(ps_url, sources['x_centroid', 'y_centroid', 'flux'], image_width, image_height, pixelscale - 0.5, pixelscale + 0.5)
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