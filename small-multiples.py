import geopandas as gpd
import matplotlib.pyplot as plt
import logging
import os
import glob
import math
import sys
import psycopg2
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# check if all required environmental variables are accessible
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB"]:
  if env_var not in os.environ:
    logging.error("❌Environmental Variable {} does not exist".format(env_var))

# temp folder
temp = "output"
if os.path.isdir(temp) != True:
  os.mkdir(temp)

def clean_temp():
  files = glob.glob(temp + '/*')
  for f in files:
      os.remove(f)

# layer_type
if (len(sys.argv) < 2):
  logging.error("❌ Command line argument for layer_type is missing")
layer_type = sys.argv[1]

# colormap
colormap = 'OrRd'
if (len(sys.argv) > 2):
  colormap = sys.argv[2]

# filename
filename = 'map.png'
if (len(sys.argv) > 3):
  filename = sys.argv[3]


pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

logging.info("🆙 Starting rendering")

with psycopg2.connect(dsn) as conn:
  value_max = 99
  min_year = 1990
  max_year = 2020

  with conn.cursor() as cur:
    cur.execute("SELECT MAX(value), MIN(value), MAX(year), MIN(year) FROM data WHERE type = %s", [layer_type])
    result = cur.fetchone()
    value_max = result[0]
    value_min = 10
    max_year = result[2]
    min_year = result[3]

  cols = 4
  rows = math.ceil((max_year-min_year) / cols)

  print(value_min, value_max, min_year, max_year, cols, rows)

  plt.figure(figsize=(40,80), facecolor='white')

  for i in range(max_year-min_year+1):

    sql = """SELECT
      grid.geom,
      data.value
    FROM
      grid
    LEFT OUTER JOIN
      data
      ON
        grid.id = data.geom_id
        AND data.year = {}
        AND data.type = '{}'
    """.format(min_year + i, layer_type)

    df = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom')
    # todo fill through neighbor value
    # df["value"] = df["value"].fillna(value_min)

    ax = plt.subplot(rows, cols, i + 1)
    df.plot(ax = ax,
      figsize=(10,20),
      column="value",
      cmap=colormap,
      antialiased=False,
      # edgecolor="face",
      # linewidth=0.4
      # color="grey",
      alpha=1,
      vmin=value_min,
      vmax=value_max
    )
    ax.set_title(min_year + i, fontsize=40, fontweight='bold')
    plt.axis('off')

    print(min_year + i)
  
  plt.tight_layout(pad=0, h_pad=.1, w_pad=.1)
  plt.savefig(temp + "/" + filename)

logging.info("🗺️ Map complete")