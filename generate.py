import sys
import os
import logging
import gzip
import json
import psycopg2
import psycopg2.extras
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

# clean_temp()

pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")

dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"

logging.info("🆙 Starting generating")

missing = 0

with psycopg2.connect(dsn) as conn:
  with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute("""CREATE FUNCTION pg_temp.ST_PointInPolygon(Geometry(Polygon, 4326)) returns TEXT AS 
      'SELECT ST_AsGeoJson(CASE
        	WHEN ST_Within(ST_Centroid($1), $1) THEN ST_Centroid($1)
        	ELSE ST_PointOnSurface($1)
        END);' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT""")
    
    cur.execute("""CREATE FUNCTION pg_temp.ST_AnchorPoints(Geometry(Polygon, 4326)) returns JSON AS 
      $$ WITH temp_bbox AS (SELECT 
        ST_Envelope($1) AS bbox
      ), temp_mid AS (
        SELECT
          ST_XMin(temp_bbox.bbox) + (ST_XMax(temp_bbox.bbox) - ST_XMin(temp_bbox.bbox)) / 2 AS midx,
          ST_YMin(temp_bbox.bbox) + (ST_YMax(temp_bbox.bbox) - ST_YMin(temp_bbox.bbox)) / 2 AS midy
        FROM
          temp_bbox
      )
      SELECT ARRAY_TO_JSON(
        ARRAY[
          -- first four points are the side-mid-points
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || temp_mid.midx || ' ' || ST_YMin(temp_bbox.bbox) || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMax(temp_bbox.bbox) || ' ' || temp_mid.midy || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || temp_mid.midx || ' ' || ST_YMax(temp_bbox.bbox) || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMin(temp_bbox.bbox) || ' ' || temp_mid.midy || ')', 4326)),
          -- last four points are the corners
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMin(temp_bbox.bbox) || ' ' || ST_YMin(temp_bbox.bbox) || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMin(temp_bbox.bbox) || ' ' || ST_YMax(temp_bbox.bbox) || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMax(temp_bbox.bbox) || ' ' || ST_YMax(temp_bbox.bbox) || ')', 4326)),
          ST_ClosestPoint($1, ST_GeomFromText('POINT(' || ST_XMax(temp_bbox.bbox) || ' ' || ST_YMin(temp_bbox.bbox) || ')', 4326))
        ]
      ) FROM temp_bbox, temp_mid; $$ LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT""")

    # cur.execute("""CREATE FUNCTION pg_temp.ST_AnchorsForPolygonIdList(integer[]) returns JSON AS 
    #   $$WITH temp AS (SELECT geom, fid, id FROM )

    #   ; $$ LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT""")
    
    # cur.execute("""CREATE FUNCTION pg_temp.ST_PointsForPolygonIdList(integer[]) returns JSON AS 
    #   $$
    #   ; $$ LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT""")

    # .....----------------------------------------

    cur.execute("SELECT type, value FROM summary_germany")
    germany_data = cur.fetchall()
    
    data_germany = []
    for r in germany_data:
      values = []
      for year in r["value"]:
        values.append([year["year"],year["min"],year["avg"],year["max"]])
      data_germany.append({
        "type": r["type"],
        "values": values
      })
    
    # .....----------------------------------------


    cur.execute("""SELECT DISTINCT plz FROM postcode""") # LIMIT 1 ALL OFFSET 2758
    postcodes = cur.fetchall()

    for postcode_dict in postcodes:
      postcode = postcode_dict["plz"]

      print(postcode)

      cur.execute("""WITH buffer AS (
          SELECT ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 5000),4326) AS buffer_geom, geom, plz FROM postcode WHERE plz = %s 
        ), fluvial AS (
          SELECT
            ST_Simplify(ST_Intersection(buffer.buffer_geom, flood_hazard.geom), 0.00005) AS geom,
            qlike AS level
          FROM 
            flood_hazard
          JOIN
            buffer
          ON ST_Intersects(buffer.buffer_geom, flood_hazard.geom)
        ), fluvial_max_size AS (
          SELECT
            DISTINCT ON (level)
            JSON_BUILD_OBJECT(
              'level',
              level, 
              'anchors',
              pg_temp.ST_AnchorPoints(geom)
            ) AS anchors
          FROM 
            fluvial
          ORDER BY
            level, ST_Area(geom) DESC
        )
        SELECT
          plz,
          pg_temp.ST_PointInPolygon(buffer.geom) AS postcode_point,
          pg_temp.ST_PointInPolygon(buffer.buffer_geom) AS postcode_buff_point,
          pg_temp.ST_AnchorPoints(buffer.geom) AS postcode_anchors,
          pg_temp.ST_AnchorPoints(buffer.buffer_geom) AS postcode_buff_anchors,
          ST_AsGeoJSON(ST_Simplify(buffer.geom, 0.00005), 4) AS postcode_geom,
          ST_AsGeoJSON(ST_Simplify(buffer.buffer_geom, 0.00005), 4) AS postcode_buff_geom,
          (
            SELECT 
              JSON_BUILD_OBJECT(
                'bbox',
                ST_AsGeoJSON(ST_Envelope(dense_spaces.geom), 3),
                'name',
                "download-ref-2015-xls_vrname"
              ) AS value
            FROM 
              dense_spaces
            JOIN
              buffer
              ON ST_Intersects(buffer.buffer_geom, dense_spaces.geom)
            ORDER BY
              ST_Area(ST_Intersection(buffer.geom, dense_spaces.geom)) DESC,
              ST_Area(ST_Intersection(buffer.buffer_geom, dense_spaces.geom)) DESC
            LIMIT 1
          ) AS dense_space,
          (
            SELECT 
              COUNT(*)
            FROM 
              flood_ocean
            JOIN
              buffer
              ON ST_Intersects(buffer.buffer_geom, flood_ocean.geom)
          ) AS has_ocean_flood,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  JSON_BUILD_OBJECT(
                    'geom',
                    ST_AsGeoJSON(geom, 4),
                    'level',
                    level
                  )
                )
              )
            FROM 
              fluvial
          ) AS fluvial_flood,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  anchors
                )
              )
            FROM
              fluvial_max_size
          ) AS fluvial_flood_anchors,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  climate_grid_zones.type
                )
              )
            FROM
              climate_grid_zones
            JOIN
            buffer
            ON ST_Intersects(buffer.buffer_geom, climate_grid_zones.geom)
          ) AS risk_zones,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  climate_grid_zones.fid
                )
              )
            FROM
              climate_grid_zones
            JOIN
            buffer
            ON ST_Intersects(buffer.buffer_geom, climate_grid_zones.geom)
          ) AS risk_zone_ids,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  JSON_BUILD_OBJECT(
                    'fid',
                    climate_grid_zones.fid,
                    'anchors',
                    pg_temp.ST_PointInPolygon(climate_grid_zones.geom)
                  )
                )
              )
            FROM
              climate_grid_zones
            JOIN
            buffer
            ON ST_Intersects(buffer.buffer_geom, climate_grid_zones.geom)
          ) AS risk_zone_points,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  JSON_BUILD_OBJECT(
                    'fid',
                    climate_grid_zones.fid,
                    'anchors',
                    pg_temp.ST_AnchorPoints(climate_grid_zones.geom)
                  )
                )
              )
            FROM
              climate_grid_zones
            JOIN
            buffer
            ON ST_Intersects(buffer.buffer_geom, climate_grid_zones.geom)
          ) AS risk_zone_anchors,
          (
            SELECT
              ARRAY_TO_JSON(
                ARRAY_AGG(
                  JSON_BUILD_OBJECT(
                    'type',
                    type,
                    'value',
                    value
                  )
                )
              )
            FROM
              summary_postcode
            WHERE
              postcode = buffer.plz::integer
            GROUP BY
              postcode
          ) AS postcode_data
        FROM
          buffer""", [postcode])

      res = cur.fetchone()

      floods = []
      # geo_floods = []

      if res['fluvial_flood']:
        for flood in res['fluvial_flood']:
          if (flood['geom'] != None):
            gj = json.loads(flood['geom'])
            if (len(gj['coordinates']) > 0):
              floods.append({
                'geom': gj,
                'level': flood['level']
              })
              # geo_floods.append(gj)

      data = {
        'postcode': int(res['plz']),
        'postcode_geom': json.loads(res['postcode_geom']),
        'postcode_buff_geom': json.loads(res['postcode_buff_geom']),
        'postcode_point': json.loads(res['postcode_point']),
        'postcode_buff_point': json.loads(res['postcode_buff_point']),
        'postcode_anchors': res['postcode_anchors'],
        'postcode_buff_anchors': res['postcode_buff_anchors'],
        'dense_space': {
          'bbox': None,
          'name': None
        },
        'fluvial_flood': floods,
        'fluvial_flood_anchors': res['fluvial_flood_anchors'],
        'has_ocean_flood':int(res['has_ocean_flood']),
        'risk_zones':res['risk_zones'],
        'risk_zone_ids':res['risk_zone_ids'],
        'risk_zone_points': res['risk_zone_points'],
        'risk_zone_anchors': res['risk_zone_anchors'],
        'data_germany': data_germany,
        'data_postcode':[]
      }

      if res['dense_space'] and res['dense_space']['bbox']:
        data['dense_space']['bbox'] = json.loads(res['dense_space']['bbox'])
      
      if res['dense_space'] and res['dense_space']['name']:
        data['dense_space']['name'] = res['dense_space']['name']

      if res['postcode_data']:
        for d in res['postcode_data']:
          values = []
          for year in d["value"]:
            values.append([year["year"],year["min"],year["avg"],year["max"]])
          data['data_postcode'].append({
            "type": d["type"],
            "values": values
          })
      else:
        missing += 1
      # geojson = {
      #   "type": "FeatureCollection",
      #   "features": [
      #   ]
      # }

      # geos = [
      #   json.loads(res['postcode_geom']),
      #   json.loads(res['postcode_buff_geom'])
      #   # json.loads(res['dense_space']['bbox'])
      # ]
      # geos = geo_floods + geos

      # for g in geos:
      #   geojson['features'].append({
      #     "type": "Feature",
      #     "properties":{},
      #     "geometry": g
      #   })

      # with open('temp/postcode_test.json', 'w') as outfile:
      #   json.dump(data, outfile)
      
      # with open('temp/postcode_test.geojson', 'w') as outfile:
      #   json.dump(geojson, outfile)

      with gzip.open('./output/postcodes/' + postcode + '.json', 'wt') as f:
        f.write(json.dumps(data))
      
      print(postcode)
      
print(missing)

logging.info("✅ Done")