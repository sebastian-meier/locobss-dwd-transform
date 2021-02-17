SELECT temp.type, ARRAY_TO_JSON(ARRAY_AGG(temp.value)) AS value FROM (
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
      (
	  
	  )
    GROUP BY
      type, year
	ORDER BY
		type ASC, year ASC
) AS temp
  GROUP BY
    type

----------------------------------------------------------------------------

WITH vgroup AS (SELECT
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
	vgroup.plz, vgroup.type