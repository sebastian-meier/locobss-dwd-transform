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
  cur.execute("""WITH vgroup AS (SELECT
	plz,
	data.type,
	JSON_BUILD_OBJECT(
		'year',
		data.year,
		'avg',
		ROUND(AVG(data.value), 2),
		'max',
		ROUND(MAX(data.value), 2),
		'min',
		ROUND(MIN(data.value), 2)
	) AS value
FROM
	postcode
JOIN
	grid
	ON ST_Intersects(postcode.geom, grid.geom)
JOIN
	data
	ON grid.id = data.geom_id
GROUP BY
	plz,
	data.type,
	data.year
ORDER BY
	data.type ASC, data.year ASC
)
INSERT INTO summary_postcode (postcode, type, value)
SELECT
	vgroup.plz::integer,
	vgroup.type,
	ARRAY_TO_JSON(ARRAY_AGG(
		vgroup.value
	))
FROM vgroup
GROUP BY
	vgroup.plz, vgroup.type""")
  
  conn.commit()
  cur.close()
 
conn.close()

logging.info("✅ Done")