import os
import psycopg2
import psycopg2.extras
import logging
import json
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

with psycopg2.connect(dsn) as conn:
  with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute("""SELECT 
      ST_AsGeoJSON(geom) AS geom,
      id,
      ags,
      '' AS data
    FROM 
      ags5""")
    
    root_geom = cur.fetchall()

    datasets = ["hot_days",
      "ice_days",
      "precipGE30mm_days",
      "snowcover_days",
      "air_temperature_max",
      "drought_index",
      "precipitation",
      "frost_days",
      "air_temperature_mean",
      "summer_days"]
    
    datatypes = [
      "min", "max", "avg"
    ]

    for dataset in datasets:

      cur.execute("""SELECT
        value,
        ags5
      FROM
        summary_ags5
      WHERE
        type = %s
      ORDER BY
        summary_ags5.value->>'year' ASC
      """, [dataset])

      data = cur.fetchall()

      for datatype in datatypes:

        geom_map = {}
        geom = root_geom

        for i, g in enumerate(geom):
          geom_map[int(g['ags'])] = i
          geom[i]['data'] = []

        year_min = 3000
        year_max = 0
        val_min = 999999999999
        val_max = 0

        for d in data:
          v = d['value']
          vs = sorted(v, key=lambda k: k['year'])
          if (vs[0]['year'] < year_min):
            year_min = vs[0]['year']
          if (vs[len(vs) - 1]['year'] > year_max):
            year_max = vs[len(vs) - 1]['year']
          for item in vs:
            if (item[datatype] > val_max):
              val_max = item[datatype]
            if (item[datatype] < val_min):
              val_min = item[datatype]
            geom[geom_map[d['ags5']]]['data'].append(item[datatype])
        
        geojson = {
          "type": "FeatureCollection",
          "features": [
          ]
        }

        first_g = True

        for g in geom:
          geometry = json.loads(g['geom'])
          for c in range(len(geometry['coordinates'])):
            for cc in range(len(geometry['coordinates'][c])):
              for ccc in range(len(geometry['coordinates'][c][cc])):
                for cccc in range(len(geometry['coordinates'][c][cc][ccc])):
                  geometry['coordinates'][c][cc][ccc][cccc] = float("{:.4f}".format(geometry['coordinates'][c][cc][ccc][cccc]))
          
          properties = { 
            "ags": g['ags'],
            "data": g['data'] 
          }

          if (first_g):
            first_g = False
            properties["year_min"] = year_min
            properties["year_max"] = year_max
            properties["value_min"] = val_min
            properties["value_max"] = val_max

          geojson['features'].append({
            "type": "Feature",
            "properties":properties,
            "geometry": geometry
          })
        
        filename = 'timeseries_' + dataset + '_' + datatype

        with open('temp/' + filename + '.geojson', 'w') as outfile:
          json.dump(geojson, outfile)
          print('geo2topo -o /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/' + filename + '.topo.json -q 1e5 /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/' + filename + '.geojson')
          print('toposimplify -p 0.00001 -F /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/' + filename + '.topo.json -o /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/final/' + filename + '.simple.topo.json')
          # os.system('geo2topo /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/' + filename + '.geojson > /home/sebastian/Sites/LoCobSS/locobss-dwd-transform/temp/' + filename + '.topo.json')
          # toposimplify -p 0.00000001 -F timeseries.topo.json -o timeseries.simple.topo.json
          # topoquantize 1e5 timeseries.simple.topo > timeseries.simple2.topo.json
