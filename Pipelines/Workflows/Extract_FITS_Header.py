
cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')


from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType, FloatType, DoubleType, BinaryType
from delta.tables import DeltaTable

fits_header_table_name = f'{cat}.{db}.fits_files_header'

schema = StructType([StructField('imageid', LongType(), True), StructField('AIRMASS', DoubleType(), True), StructField('ANGLE', DoubleType(), True), StructField('AOCAMBT', DoubleType(), True), StructField('AOCBAROM', DoubleType(), True), StructField('AOCCLOUD', LongType(), True), StructField('AOCDEW', DoubleType(), True), StructField('AOCHUM', LongType(), True), StructField('AOCRAIN', LongType(), True), StructField('AOCWIND', DoubleType(), True), StructField('AOCWINDD', LongType(), True), StructField('BITPIX', LongType(), True), StructField('BSCALE', LongType(), True), StructField('BZERO', LongType(), True), StructField('CCD-TEMP', LongType(), True), StructField('CCDXBIN', LongType(), True), StructField('CCDYBIN', LongType(), True), StructField('CENTALT', DoubleType(), True), StructField('CREATOR', StringType(), True), StructField('CRPIX1', LongType(), True), StructField('CRPIX2', LongType(), True), StructField('CRVAL1', DoubleType(), True), StructField('CRVAL2', DoubleType(), True), StructField('CTYPE1', StringType(), True), StructField('CTYPE2', StringType(), True), StructField('DATE-LOC', StringType(), True), StructField('DATE-OBS', StringType(), True), StructField('DEC', DoubleType(), True), StructField('EGAIN', LongType(), True), StructField('EXPOSURE', LongType(), True), StructField('FILTER', StringType(), True), StructField('FLIPPED', BooleanType(), True), StructField('FOCALLEN', LongType(), True), StructField('FOCPOS', LongType(), True), StructField('FOCTEMP', DoubleType(), True), StructField('FOCUSER', StringType(), True), StructField('FWHEEL', StringType(), True), StructField('GAIN', LongType(), True), StructField('IMAGETYP', StringType(), True), StructField('INSTRUME', StringType(), True), StructField('NAXIS', LongType(), True), StructField('NAXIS1', LongType(), True), StructField('NAXIS2', LongType(), True), StructField('OBJCTALT', DoubleType(), True), StructField('OBJCTDEC', StringType(), True), StructField('OBJCTRA', StringType(), True), StructField('OBJECT', StringType(), True), StructField('OBSERVER', StringType(), True), StructField('PIXSCALE', DoubleType(), True), StructField('RA', DoubleType(), True), StructField('SCALE', DoubleType(), True), StructField('SET-TEMP', LongType(), True), StructField('SIMPLE', BooleanType(), True), StructField('SITEELEV', LongType(), True), StructField('SITELAT', StringType(), True), StructField('SITELONG', StringType(), True), StructField('SITENAME', StringType(), True), StructField('TELESCOP', StringType(), True), StructField('XBINNING', LongType(), True), StructField('XPIXSZ', DoubleType(), True), StructField('YBINNING', LongType(), True), StructField('YPIXSZ', DoubleType(), True)])
fits_header_tbl = DeltaTable.createIfNotExists(spark).tableName(fits_header_table_name).addColumns(schema).execute()


from pyspark.sql.functions import *
import pandas as pd

@pandas_udf('string')
def extract_header(fitscontent: pd.Series) -> pd.Series:
  from astropy.io import fits
  import json
  headers_list = []
  for fc in fitscontent:
    cards = {}
    hdu = fits.PrimaryHDU.fromstring(bytes(fc), ignore_missing_end=False)
    header = hdu.header
    while len(header) > 0:
      i = header.popitem()
      cards[i[0]] = i[1]
    headers_list.append(json.dumps(cards))
  return pd.Series(headers_list)
  
spark.udf.register(name='extract_header',f=extract_header)


dfFITSheaderCards = spark.sql(f'select imageid, fits_header from (select imageid, extract_header(content) as fits_header from {cat}.{db}.fits_files_raw)')
json_schema = dfFITSheaderCards.select(schema_of_json(dfFITSheaderCards.fits_header)).first()[0]
dfFITSheader = dfFITSheaderCards.withColumn('struct_col', from_json(col('fits_header'), json_schema))
dfFITSheader.createOrReplaceTempView('dffitsheader')


cmd = f'insert into {fits_header_table_name} select imageid, struct_col.* from dffitsheader where imageid not in (select imageid from {fits_header_table_name})'
spark.sql(cmd)