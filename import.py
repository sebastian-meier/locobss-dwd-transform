import sys
import os
import logging
import gzip
import shutil
import psycopg2
import psycopg2.extras
import geopandas
import subprocess
import glob
import re
import datetime
from shapely.wkt import dumps
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# check if all required environmental variables are accessible
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB", "INPUT_FOLDER", "YEAR_LIMIT"]:
  if env_var not in os.environ:
    logging.error("❌Environmental Variable {} does not exist".format(env_var))

# temp folder
temp = "temp"
if os.path.isdir(temp) != True:
  os.mkdir(temp)

def clean_temp():
  files = glob.glob(temp + '/*')
  for f in files:
      os.remove(f)

clean_temp()

# grid file
if (len(sys.argv) < 2):
  logging.error("❌ Command line argument for folders is missing")

folders = sys.argv[1].split(',')
input_folder = os.getenv("INPUT_FOLDER")

for folder in folders:
  if os.path.isdir(input_folder + folder) != True:
    logging.error("❌ Folder {} is not a valid path".format(input_folder + folder))

pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

logging.info("🆙 Starting import")

insert_count = 0
for folder in folders:
  files = glob.glob(input_folder + folder + '/*.asc.gz')
  for f in files:
    year = int(re.findall(r"\D(\d{4})", f)[0])
    start_time = datetime.datetime.now()

    if year < int(os.getenv("YEAR_LIMIT")):

      with gzip.open(f, 'rb') as f_in:
        with open(temp + '/grid.asc', 'wb') as f_out:
          shutil.copyfileobj(f_in, f_out)

          cmdline = [
            "gdal_polygonize.py",
            temp + "/grid.asc",
            "-f", "ESRI Shapefile",
            temp + "/grid.shp",
            "temp",
            "MYFLD",
            "-q"
          ]

          subprocess.call(cmdline)

          df = geopandas.read_file(temp + "/grid.shp")
          df = df.set_crs("epsg:31467")
          df = df.to_crs("epsg:4326")

          if df['geometry'].count() > 0:
            clean = df[(df['MYFLD'].notnull())] # (df['MYFLD'] > 0) & 
            if len(clean) > 0:
              values = []
              for index, row in clean.iterrows():
                values.append([dumps(row.geometry, rounding_precision=5), row.MYFLD, year, folder])
              
              with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                  cur.execute("DELETE FROM data_temp;")
                  
                  psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO data_temp (geom, value, year, type) VALUES %s;",
                    values,
                    template="(ST_GeomFromText(%s, 4326), %s, %s, %s)",
                    page_size=1000
                  )

                  cur.execute("""INSERT INTO 
                    data (
                      geom_id, 
                      value, 
                      year, 
                      type
                  ) SELECT 
                    grid.id,
                    data_temp.value,
                    data_temp.year,
                    data_temp.type
                  FROM
                    grid
                  JOIN
                    data_temp
                    ON
                      ST_WithIn(grid.centroid, data_temp.geom)
                  ;""")

                  cur.execute("DELETE FROM data_temp;")

              end_time = datetime.datetime.now()
              insert_count += 1
              print("INSERT {}, {}, {}, {}, {}".format(folder, year, len(values), insert_count, (end_time - start_time).microseconds))

              values = None
            clean = None
          df = None
      clean_temp()

logging.info("✅ Data imported")