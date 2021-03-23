# For importin additional drought data: https://www.ufz.de/index.php?de=37937

import rioxarray
import xarray
import pandas
import matplotlib.pyplot as plt
import subprocess
import geopandas as pd
import math
import os
from shapely.geometry import Point
import psycopg2
import logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# check if all required environmental variables are accessible
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB"]:
  if env_var not in os.environ:
    logging.error("❌Environmental Variable {} does not exist".format(env_var))

# https://github.com/geopandas/geopandas/issues/1565
import warnings
warnings.filterwarnings("ignore", "The GeoSeries you are attempting", UserWarning)

pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

sql = "SELECT id AS geom_id, geom FROM grid"
grid = None

with psycopg2.connect(dsn) as conn:
  grid = pd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom')

# https://www.ufz.de/index.php?de=47252
# https://www.ufz.de/index.php?de=37937
# 248981_SMI_SM_Lall_Gesamtboden_monatlich_1951-2020_inv.nc
# 248980_SMI_SM_L02_Oberboden_monatlich_1951-2020_inv.nc
xds = xarray.open_dataset('./input/248981_SMI_SM_Lall_Gesamtboden_monatlich_1951-2020_inv.nc')
# xds.rio.set_crs(4326)

annual_sample = xds.resample(time='1Y').mean()

annual_smi = annual_sample['SMI'][:]
smi_max = annual_smi.max().values
smi_min = annual_smi.min().values

print(smi_max, smi_min)

timestamps = annual_sample["time"]

max = 0
plt.figure(figsize=(40,100), facecolor='white')
cols = 6
rows = math.ceil(len(timestamps) / cols)

i = 0
for _year in timestamps:
  year = _year.values
  year_data = annual_sample.sel(time=year)
  smi = year_data['SMI'][:]
  _max = smi.max().values
  if (_max > max):
    max = _max
  print(pandas.to_datetime(year).year, ':', _max)

  df = year_data.to_dataframe()
  df = df.reset_index()
  df = df.drop(columns=['easting', 'northing', 'time'])
  gdf = pd.GeoDataFrame(df, geometry=pd.points_from_xy(df.lon, df.lat))
  gdf = gdf.set_crs(epsg=4326)
  gdf = gdf.to_crs(epsg=3857)
  gdf.geometry = gdf.geometry.buffer(3000)
  gdf = gdf.to_crs(epsg=4326)
  gdf = gdf.drop(columns=['lat', 'lon'])

  # gdf = gdf.dropna(subset=['SMI'])
  # gdf.to_crs(epsg=3857)

  merged_grid = pd.sjoin(gdf, grid, how="inner", op='intersects')
  merged_grid = merged_grid.drop(columns=['index_right'])

  grouped_grid = merged_grid.groupby(['geom_id']).mean()

  grid_merge = grouped_grid.merge(grid, how='left', on="geom_id")
  
  ngdf = pd.GeoDataFrame(grid_merge, geometry=grid_merge['geom'])
  ngdf = ngdf.set_crs(epsg=4326)
  ngdf = ngdf.to_crs(epsg=3857)
  ngdf = ngdf.drop_duplicates(subset=['geom_id'])
  ngdf.drop(columns=['geom', 'geom_id'])
  ngdf_filtered = ngdf[ngdf.area > 0]

#   year_data["SMI"].rio.to_raster('./input/geotif/{}.tif'.format(pandas.to_datetime(year).year))

# i = 0
# for _year in timestamps:
#   year = _year.values

#   cmdline = [
#     "gdal_polygonize.py",
#     "./input/geotif/{}.tif".format(pandas.to_datetime(year).year),
#     "-f", "ESRI Shapefile",
#     "./input/nc.shp",
#     "temp",
#     "MYFLD",
#     "-q"
#   ]

#   subprocess.call(cmdline)

#   shape = pd.read_file("./input/nc.shp")
#   # shape['MYFLD'] = shape['MYFLD'].astype(float)
#   # shape['MYFLD'] = shape[(shape['MYFLD'] > 0)]
#   # shape['MYFLD'] = shape['MYFLD'].fillna(0)

#   print(shape['MYFLD'].min(), shape['MYFLD'].max())

  ax = plt.subplot(rows, cols, i + 1)
  ngdf.plot(ax = ax,
      # figsize=(10,20),
      column="SMI",
      cmap='Reds_r',
      antialiased=False,
      # edgecolor="face",
      # linewidth=0.4
      # color="grey",
      alpha=1,
      vmin=0.0,
      vmax=smi_max,
      missing_kwds = {
        "color": "white"
      }
  )

  ax.set_title(pandas.to_datetime(year).year, fontsize=40, fontweight='bold')
  plt.axis('off')
  i += 1

  # os.remove('./input/nc.shp')
  # os.remove('./input/nc.dbf')
  # os.remove('./input/nc.shx')

plt.tight_layout(pad=0, h_pad=.1, w_pad=.1)
plt.savefig("./input/drought.png")