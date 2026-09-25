{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "766b20ba-dd5d-4ed1-bb45-b6e2fc328c0e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import col, desc\n",
    "from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType, FloatType\n",
    "from delta.tables import *\n",
    "import pandas as pd\n",
    "import astrolib.platesolve as ps\n",
    "from astropy.table import Table\n",
    "import numpy as np\n",
    "\n",
    "cat = dbutils.widgets.get('cat')\n",
    "db = dbutils.widgets.get('db')\n",
    "\n",
    "plate_solve_table = f'{cat}.{db}.fits_files_plate_solutions'\n",
    "fits_headers = f'{cat}.{db}.fits_files_header'\n",
    "centroids_table = f'{cat}.{db}.fits_files_centroids'\n",
    "\n",
    "images = spark.sql(f'select imageid, naxis1, naxis2, pixscale from {fits_headers} as h where not exists (select * from {plate_solve_table} as s where h.imageid = s.imageid and s.simple is true)').collect()\n",
    "#image_solutions = []\n",
    "\n",
    "\n",
    "for image in images:\n",
    "  sources = None\n",
    "  imageid = image[0]\n",
    "  image_width = image[1]\n",
    "  image_height = image[2]\n",
    "  pixelscale = image[3]\n",
    "  sources = spark.read.table(centroids_table).where(col('imageid') == imageid).orderBy(col('flux').desc()).toPandas()\n",
    "  print(f'Image {imageid} has {len(sources)} sources')\n",
    "  if type(sources) is pd.DataFrame:\n",
    "    sources = Table.from_pandas(sources)\n",
    "  h = ps.solve_from_source_list_api(sources['xcentroid', 'ycentroid', 'flux'], image_width, image_height, pixelscale - 0.02, pixelscale + 0.02)\n",
    "  column_str = f'{imageid},' # For using SQL INSERT\n",
    "  image_solution = [imageid]\n",
    "  for c in h.header.items():\n",
    "    if c[0] != 'COMMENT' and c[0] != 'HISTORY' and c[0] != 'DATE':\n",
    "      if type(c[1]) is str:\n",
    "        column_str += f\"'{c[1]}',\"\n",
    "      else:\n",
    "          column_str += f'{c[1]},'\n",
    "      image_solution.append(c[1])\n",
    "\n",
    "  sqlcmd = f'insert into {plate_solve_table} values ({column_str[:-1]})'\n",
    "  print(sqlcmd) # for debugging\n",
    "  spark.sql(sqlcmd)\n",
    "  #image_solutions.append(image_solution)"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "Local_Plate_solve",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}