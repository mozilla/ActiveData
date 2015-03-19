-- CHUNK TIMING FOR THAT PAST FULL WEEK
-- WITH SELECT COUNT(1) FROM test_results == 1,156,126,589
-- THIS QUERY TAKES 364 row(s) retrieved starting from 0 in 268/257515 ms

SELECT
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk",
	COUNT("run.stats.duration"),
	AVG("run.stats.duration")
FROM
	test_results
WHERE
	"etl.id" = 0 AND -- SINCE THE CUBE IS OF TESTS, WE PICK TEST 0 AS SUITE REPRESENTATIVE
	date_trunc('week', dateadd(DAY, -7, GETDATE())) <= TIMESTAMP 'epoch' + "run.stats.start_time" * INTERVAL '1 second' AND
	TIMESTAMP 'epoch' + "run.stats.start_time" * INTERVAL '1 second' < date_trunc('week', GETDATE())
GROUP BY
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk"
ORDER BY
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk"
;
