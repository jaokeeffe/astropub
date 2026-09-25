# Databricks notebook source
# MAGIC %md
# MAGIC ### From the point sources, solve the image using Astrometry.NET

# COMMAND ----------

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${cat}.${db}.fits_files_plate_solutions
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

from astroquery.astrometry_net import AstrometryNet
from pyspark.sql.functions import col, desc
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType, FloatType
from delta.tables import *
import pandas as pd

source_count = 50
scale_err = 5

key = dbutils.secrets.get('scratchkv', 'astrometry-net-api-key')
ast = AstrometryNet()
ast.TIMEOUT = 600
ast.api_key = key

plateSolutionSchema = StructType(
  [
    StructField('imageid', LongType()),
    StructField('simple', BooleanType()),
    StructField('bitpix', IntegerType()),
    StructField('naxis', IntegerType()),
    StructField('extend', BooleanType()),
    StructField('wcsaxes', IntegerType()),
    StructField('ctype1', StringType()),
    StructField('ctype2', StringType()),
    StructField('equinox', FloatType()),
    StructField('lonpole', FloatType()),
    StructField('latpole', FloatType()),
    StructField('crval1', FloatType()),
    StructField('crval2', FloatType()),
    StructField('crpix1', FloatType()),
    StructField('crpix2', FloatType()),
    StructField('cunit1', StringType()),
    StructField('cunit2', StringType()),
    StructField('cd1_1', FloatType()),
    StructField('cd1_2', FloatType()),
    StructField('cd2_1', FloatType()),
    StructField('cd2_2', FloatType()),
    StructField('imagew', IntegerType()),
    StructField('imageh', IntegerType()),
    StructField('a_order', IntegerType()),
    StructField('a_0_0', FloatType()),
    StructField('a_0_1', FloatType()),
    StructField('a_0_2', FloatType()),
    StructField('a_1_0', FloatType()),
    StructField('a_1_1', FloatType()),
    StructField('a_2_0', FloatType()),
    StructField('b_order', IntegerType()),
    StructField('b_0_0', FloatType()),
    StructField('b_0_1', FloatType()),
    StructField('b_0_2', FloatType()),
    StructField('b_1_0', FloatType()),
    StructField('b_1_1', FloatType()),
    StructField('b_2_0', FloatType()),
    StructField('ap_order', IntegerType()),
    StructField('ap_0_0', FloatType()),
    StructField('ap_0_1', FloatType()),
    StructField('ap_0_2', FloatType()),
    StructField('ap_1_0', FloatType()),
    StructField('ap_1_1', FloatType()),
    StructField('ap_2_0', FloatType()),
    StructField('bp_order', IntegerType()),
    StructField('bp_0_0', FloatType()),
    StructField('bp_0_1', FloatType()),
    StructField('bp_0_2', FloatType()),
    StructField('bp_1_0', FloatType()),
    StructField('bp_1_1', FloatType()),
    StructField('bp_2_0', FloatType())
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loop through images and upload centroids for astrometric solution
# MAGIC Calculating an astrometric solution is time consuming. Avoid unnecessary processing by only sending images not already solved.

# COMMAND ----------

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
  (h, sid) = ast.solve_from_source_list(sources[:source_count]['xcentroid'], sources[:source_count]['ycentroid'], image_width=image_width, image_height=image_height, scale_units='arcsecperpix', scale_type='ev', scale_est=pixelscale, scale_err=scale_err, crpix_center=False, solve_timeout=600, return_submission_id=True)
  column_str = f'{imageid},' # For using SQL INSERT
  image_solution = [imageid]
  for c in h.items():
    if c[0] != 'COMMENT' and c[0] != 'HISTORY' and c[0] != 'DATE':
      if type(c[1]) is str:
        column_str += f"'{c[1]}',"
      else:
          column_str += f'{c[1]},'
      image_solution.append(c[1])

  sqlcmd = f'insert into {plate_solve_table} values ({column_str[:-1]})'
  print(f'Solved Submission ID: {sid}')
  #spark.sql(sqlcmd)
  #image_solutions.append(image_solution)

# COMMAND ----------

#dfPlateSolutions = spark.createDataFrame(pd.DataFrame(image_solutions), schema=plateSolutionSchema)
#tblPlateSolveSolutions = DeltaTable.forName(spark, plate_solve_table)
#(tblPlateSolveSolutions.alias('target')
#  .merge(dfPlateSolutions.alias('source'), 'target.imageid = source.imageid')
#  .whenNotMatchedInsertAll()
#  .whenMatchedUpdateAll() # shouldn't really get here...
#  .execute()
#)