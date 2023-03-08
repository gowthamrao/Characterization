--need to know indication/target/outcome tuples
drop table if exists #targets_agg;
select * into #targets_agg
from @target_database_schema.@target_table
where cohort_definition_id in
(@target_ids);

drop table if exists #outcomes_agg;
select * into #outcomes_agg
from @outcome_database_schema.@outcome_table
where cohort_definition_id in
(@outcome_ids);

-- create all the cohort details
drop table if exists #cohort_details;

WITH cohort_combis
AS (
	SELECT DISTINCT t.cohort_definition_id AS target_cohort_id,
		o.cohort_definition_id AS outcome_cohort_id
	FROM (
		SELECT DISTINCT cohort_definition_id
		FROM #targets_agg
		) AS t
	CROSS JOIN (
		SELECT DISTINCT cohort_definition_id
		FROM #outcomes_agg
		) AS o
	)
SELECT *,
	ROW_NUMBER() OVER (
		ORDER BY cohort_type,
			target_cohort_id,
			outcome_cohort_id
		) AS cohort_definition_id
INTO #cohort_details
FROM (
	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'TnO' AS cohort_type -- T with O indexed at T
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'OnT' AS cohort_type -- T with O indexed at O
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'TnOc' AS cohort_type -- T without O
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		0 outcome_cohort_id,
		'T' AS cohort_type -- T only
	FROM cohort_combis

	UNION

	SELECT DISTINCT 0 target_cohort_id,
		outcome_cohort_id,
		'O' AS cohort_type -- O only
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'TiOT' AS cohort_type -- T overlaps O indexed at T
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'TiOO' AS cohort_type -- T overlaps O indexed at O
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'OiTT' AS cohort_type -- O overlaps T indexed at T
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		outcome_cohort_id,
		'OiTO' AS cohort_type -- O overlaps T indexed at O
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		0 AS outcome_cohort_id,
		'Tf' AS cohort_type -- T earliest event
	FROM cohort_combis

	UNION

	SELECT DISTINCT 0 AS target_cohort_id,
		outcome_cohort_id,
		'Of' AS cohort_type -- O earliest event
	FROM cohort_combis

	UNION

	SELECT DISTINCT target_cohort_id,
		0 AS outcome_cohort_id,
		'Tl' AS cohort_type -- T latest
	FROM cohort_combis

	UNION

	SELECT DISTINCT 0 AS target_cohort_id,
		outcome_cohort_id,
		'Ol' AS cohort_type -- O latest
	FROM cohort_combis
	) TEMP;


-- 1) get all the people with the outcome in TAR
drop table if exists #target_with_outcome;

-- TnO
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_with_outcome
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);


-- 2) get all the people without the outcome in TAR
drop table if exists #target_nooutcome;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_nooutcome
from #targets_agg t
CROSS JOIN
( select distinct cohort_definition_id from #outcomes_agg) o
left outer join #target_with_outcome two
on t.cohort_definition_id = two.target_cohort_id
and t.subject_id = two.subject_id
and o.cohort_definition_id = two.outcome_cohort_id
where two.subject_id IS NULL;

-- 3) get all the events where target overlaps outcome
drop table if exists #t_overlaps_o;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #t_overlaps_o
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- target starts before outcome end
t.cohort_start_date <= o.cohort_end_date
and
-- outcome starts after target start
t.cohort_end_date >= o.cohort_start_date;

-- 4) get all the events where outcome overlaps target
drop table if exists #o_overlaps_t;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #o_overlaps_t
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- outcomes starts before target end
o.cohort_start_date <= o.cohort_end_date
and
-- target starts after outcome start
o.cohort_end_date >= t.cohort_start_date;

-- Final: select into #agg_cohorts

drop table if exists #agg_cohorts;
select * into #agg_cohorts

from
(
-- T with O indexed at T

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #target_with_outcome tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnO'

union all

-- T with O indexed at O

select
tno.subject_id,
tno.outcome_start_date as cohort_start_date,
tno.outcome_end_date as cohort_end_date,
cd.cohort_definition_id
from #target_with_outcome tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'OnT'

union all

-- T without O

select
tnoc.subject_id,
tnoc.cohort_start_date,
tnoc.cohort_end_date,
cd.cohort_definition_id
from #target_nooutcome tnoc
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tnoc.target_cohort_id
and cd.outcome_cohort_id = tnoc.outcome_cohort_id
and cd.cohort_type = 'TnOc'

union all

-- Ts and Os

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_agg as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'T' -- Target

union all

-- Outcome

select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'O'

union all

-- T earliest event

select
t.subject_id,
min(t.cohort_start_date) cohort_start_date,
min(t.cohort_end_date) cohort_end_date,
cd.cohort_definition_id
from #targets_agg as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'Tf'
GROUP BY t.subject_id,
cd.cohort_definition_id

union all

-- O earliest event

select
o.subject_id,
min(o.cohort_start_date) cohort_start_date,
min(o.cohort_end_date) cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'Of'
GROUP BY o.subject_id,
cd.cohort_definition_id

union all

-- T latest event

select
t.subject_id,
max(t.cohort_start_date) cohort_start_date,
max(t.cohort_end_date) cohort_end_date,
cd.cohort_definition_id
from #targets_agg as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'Tl'
GROUP BY t.subject_id,
cd.cohort_definition_id

union all

-- O latest event

select
o.subject_id,
max(o.cohort_start_date) cohort_start_date,
max(o.cohort_end_date) cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'Ol'
GROUP BY o.subject_id,
cd.cohort_definition_id

union all

-- T overlaps O indexed at T
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #t_overlaps_o t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'TiOT'

union all

-- T overlaps O indexed at O
select
t.subject_id,
t.outcome_start_date,
t.outcome_end_date,
cd.cohort_definition_id
from #t_overlaps_o t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'TiOO'

union all

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #o_overlaps_t t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'OiTT' -- O overlaps T indexed at T

union all

select
t.subject_id,
t.outcome_start_date,
t.outcome_end_date,
cd.cohort_definition_id
from #o_overlaps_t t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'OiTO' -- O overlaps T indexed at O

) temp_ts2;
