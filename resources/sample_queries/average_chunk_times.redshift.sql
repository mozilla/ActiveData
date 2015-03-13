select
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk",
	count("run.stats.duration"),
	avg("run.stats.duration")
from
	test_result
where
	"etl.id"=0 and
	date_trunc('week', dateadd(DAY, -7, GETDATE())) <= TIMESTAMP 'epoch' + "run.stats.start_time" * INTERVAL '1 second' and
	TIMESTAMP 'epoch' + "run.stats.start_time" * INTERVAL '1 second' < date_trunc('week', GETDATE())
group by
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk"
order by
	"machine.platform",
	"machine.os",
	"run.suite",
	"run.chunk"
