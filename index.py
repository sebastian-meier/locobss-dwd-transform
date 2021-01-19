import gzip
import shutil
import subprocess
from shapely.wkt import dumps
import geopandas
import psycopg2
import psycopg2.extras

pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

logging.info("🆙 Starting harvester v0.5")

# get last day of insert
last_date = None

try:
  conn = psycopg2.connect(dsn)
  logging.info("🗄 Database connection established")
except:
  logging.error("❌Could not establish database connection")
  conn = None

with conn.cursor() as cur:
  cur.execute("SELECT collection_date FROM radolan_harvester WHERE id = 1")
  last_date = cur.fetchone()[0]

logging.info("Last harvest {}".format(last_date))

# create a temporary folder to store the downloaded DWD data
path = "/temp/"
if os.path.isdir(path) != True:
  os.mkdir(path)

with gzip.open('input/grids_germany_annual_hot_days_2020_17.asc.gz', 'rb') as f_in:
    with open('output/grids_germany_annual_hot_days_2020_17.asc', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

        cmdline = [
            'gdal_polygonize.py',
            'output/grids_germany_annual_hot_days_2020_17.asc',
            "-f", "ESRI Shapefile",
            "output/temp.shp",
            "temp",
            "MYFLD"
        ]

        subprocess.call(cmdline)

        df = geopandas.read_file("output/temp.shp")
        df = df.set_crs("epsg:31467")
        df = df.to_crs("epsg:3857")

        if df['geometry'].count() > 0:
            # for initial build take 
            clean = df[(df['MYFLD'] > 0) & (df['MYFLD'].notnull())]
            if len(clean) > 0:
                values = []
                for index, row in clean.iterrows():
                    values.append([dumps(row.geometry, rounding_precision=5), row.MYFLD, date_time_obj])

                with conn.cursor() as cur:
                    cur.execute("DELETE FROM radolan_temp;")
                    psycopg2.extras.execute_batch(
                        cur,
                        "INSERT INTO radolan_temp (geometry, value, measured_at) VALUES (ST_Multi(ST_Transform(ST_GeomFromText(%s, 3857), 4326)), %s, %s);",
                        values
                    )
                    # in order to keep our database fast and small, we are not storing the original polygonized data, but instead we are using a grid and only store the grid ids and the corresponding precipitation data
                    cur.execute("INSERT INTO radolan_data (geom_id, value, measured_at) SELECT radolan_geometry.id, radolan_temp.value, radolan_temp.measured_at FROM radolan_geometry JOIN radolan_temp ON ST_WithIn(radolan_geometry.centroid, radolan_temp.geometry);")
                    cur.execute("DELETE FROM radolan_temp;")
                    conn.commit()