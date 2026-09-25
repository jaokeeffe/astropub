# Old style AutoLoaded - to be converted to LDP

cat = dbutils.widgets.get('cat')
db = dbutils.widgets.get('db')


images_folder = f'/volumes/{cat}@{db}/incoming'
checkpoint_folder = f'/volumes/{cat}@{db}/checkpoints/incoming/fits_auto_loader'

(spark.readStream
  .format('cloudFiles')
  .option('cloudFiles.format', 'binaryfile')
  .option('cloudFiles.schemaLocation', checkpoint_folder)
  .load(images_folder)
  .select('path', 'modificationTime', 'length', 'content')
  .writeStream
  .option('checkpointLocation', checkpoint_folder)
  .trigger(availableNow=True)
  .toTable(f'{cat}.{db}.fits_files_raw'))