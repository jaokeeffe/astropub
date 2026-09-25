# Databricks notebook source
# MAGIC %md
# MAGIC ### Identify the point sources (centroids) in the data and write to a new table

# COMMAND ----------

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')

# COMMAND ----------

# MAGIC %sql create table if not exists ${cat}.${db}.fits_files_centroids (imageid long, centroidid long, xcentroid double, ycentroid double, sharpness double, roundness1 double, roundness2 double, npix long, peak double, flux double, mag double)

# COMMAND ----------

from astropy.stats import sigma_clipped_stats
from photutils.detection import DAOStarFinder
from astropy.io import fits
import numpy as np
from delta.tables import *
from pyspark.sql.functions import lit

fits_centroids_table_name = f'{cat}.{db}.fits_files_centroids'

objectdata = spark.sql(f'select h.imageid, h.ra, h.dec, h.naxis1, h.naxis2, d.fits_data from {cat}.{db}.fits_files_data d inner join {cat}.{db}.fits_files_header h on d.imageid = h.imageid where d.imageid not in (select imageid from {fits_centroids_table_name})').collect()

centroid_table = DeltaTable.forName(spark, fits_centroids_table_name)

for i in objectdata:
  naxis1 = i['naxis1']
  naxis2 = i['naxis2']
  fitsdata = i['fits_data']
  imageid = i['imageid']
  fitsimage = np.frombuffer(fitsdata, dtype=np.uint16).reshape(naxis2, naxis1)
  mean, median, std = sigma_clipped_stats(fitsimage, sigma=3.0)
  daofind = DAOStarFinder(fwhm=3.0, threshold=5.*std, exclude_border=True)
  sources = daofind(fitsimage - median)
  dfSources = spark.createDataFrame(sources.to_pandas()).withColumnRenamed('id','centroidid').withColumn('imageid',lit(imageid))
  centroid_table.alias('existing') \
    .merge(dfSources.alias('new'), 'existing.imageid = new.imageid and existing.centroidid = new.centroidid') \
    .whenNotMatchedInsert(values =
                          {
                            'imageid': 'new.imageid',
                            'centroidid': 'new.centroidid',
                            'xcentroid': 'new.xcentroid',
                            'ycentroid': 'new.ycentroid',
                            'sharpness': 'new.sharpness',
                            'roundness1': 'new.roundness1',
                            'roundness2': 'new.roundness2',
                            'npix': 'new.npix',
                            'peak': 'new.peak',
                            'flux': 'new.flux',
                            'mag': 'new.mag'
                          }
                          ).execute()

# COMMAND ----------

