import os
import logging
import psycopg2
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# check if all required environmental variables are accessible
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB"]:
  if env_var not in os.environ:
    logging.error("❌Environmental Variable {} does not exist".format(env_var))

pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

logging.info("🆙 Starting setup")

try:
  conn = psycopg2.connect(dsn)
  logging.info("🗄 Database connection established")
except:
  logging.error("❌Could not establish database connection")
  conn = None

with conn.cursor() as cur:
  cur.execute("""CREATE TABLE IF NOT EXISTS public.grid (
      id SERIAL PRIMARY KEY,
      geom GEOMETRY(POLYGON, 4326),
      centroid GEOMETRY(POINT, 4326)
    );""")
  
  cur.execute("""CREATE TABLE IF NOT EXISTS public.data (
      id SERIAL PRIMARY KEY,
      geom_id INTEGER,
      type TEXT,
      year INTEGER,
      value INTEGER,
      CONSTRAINT data_geom_fk
        FOREIGN KEY (geom_id)
          REFERENCES grid(id)
    );""")
  
  cur.execute("""CREATE TABLE IF NOT EXISTS public.data_temp (
      id SERIAL PRIMARY KEY,
      geom GEOMETRY(POLYGON, 4326),
      type TEXT,
      year INTEGER,
      value INTEGER
    );""")
  
  # add later for better performance
  # cur.execute("CREATE INDEX IF NOT EXISTS data_geom_id_idx ON data (geom_id)")
  # cur.execute("CREATE INDEX IF NOT EXISTS data_type_idx ON data (type)")
  cur.execute("CREATE INDEX IF NOT EXISTS grid_geom_idx ON grid USING GIST (geom)")
  cur.execute("CREATE INDEX IF NOT EXISTS grid_centroid_idx ON grid USING GIST (centroid)")
  cur.execute("CREATE INDEX IF NOT EXISTS data_temp_geom_idx ON data_temp USING GIST (geom)")

  cur.execute("""CREATE TABLE IF NOT EXISTS public.summary_germany (
      id SERIAL PRIMARY KEY,
      type TEXT,
      value JSON
    );""")
  
  cur.execute("""CREATE TABLE IF NOT EXISTS public.postcode (
      postcode INTEGER PRIMARY KEY,
      geom GEOMETRY(POLYGON, 4326),
      buffer GEOMETRY(POLYGON, 4326)
    );""")
  
  cur.execute("CREATE INDEX IF NOT EXISTS postcode_geom_idx ON postcode USING GIST (geom)")
  cur.execute("CREATE INDEX IF NOT EXISTS postcode_buffer_idx ON postcode USING GIST (buffer)")

  cur.execute("""CREATE TABLE IF NOT EXISTS public.municipality (
      id SERIAL PRIMARY KEY,
      
      geom GEOMETRY(POLYGON, 4326)
    );""")
  
  cur.execute("CREATE INDEX IF NOT EXISTS municipality_geom_idx ON municipality USING GIST (geom)")

  cur.execute("""CREATE TABLE IF NOT EXISTS public.summary_postcode (
      id SERIAL PRIMARY KEY,
      postcode INTEGER,
      type TEXT,
      value JSON
    );""")

  conn.commit()
  cur.close()
 
conn.close()

logging.info("✅ Done")