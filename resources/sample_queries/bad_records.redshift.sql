select distinct
	"etl.source.source.id"
from
	test_results
where
	"build.date" is null
order by
	"etl.source.source.id" DESC
;


-- BAD BECAUSE id DOES NOT START AT ZERO
-- NOT TECHNICALLY WRONG EITHER
SELECT
	"etl.source.source.id",
	MAX(min_id) max_min_id
FROM (
		SELECT
			"etl.source.source.id",
			"etl.source.source.source.id",
			min("etl.source.id") min_id
		FROM
			test_results
		GROUP BY
			"etl.source.source.id",
			"etl.source.source.source.id"
	) a
GROUP BY
	"etl.source.source.id"
ORDER BY
	"etl.source.source.id" DESC
;
