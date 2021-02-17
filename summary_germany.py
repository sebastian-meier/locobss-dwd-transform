import sys
import os
import logging
import psycopg2
import psycopg2.extras
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

logging.info("🆙 Starting summary for Germany")

try:
  conn = psycopg2.connect(dsn)
  logging.info("🗄 Database connection established")
except:
  logging.error("❌Could not establish database connection")
  conn = None

with conn.cursor() as cur:
  cur.execute("""INSERT INTO summary_germany (type, value)
  (SELECT temp.type, ARRAY_TO_JSON(ARRAY_AGG(temp.value)) AS value FROM (
	  SELECT 
      type,
      JSON_BUILD_OBJECT(
        'year',
            year,
        'avg',
            ROUND(AVG(value), 2),
        'max',
            ROUND(MAX(value), 2),
        'min',
            ROUND(MIN(value), 2)
      ) AS value
    FROM
      data
    GROUP BY
      type, year
	  ORDER BY
		  type ASC,
      year ASC
  ) AS temp
  GROUP BY
    type)""")
  
  conn.commit()
  cur.close()
 
conn.close()

logging.info("✅ Done")