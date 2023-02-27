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

select *,
ROW_NUMBER() OVER (ORDER BY cohort_type, target_cohort_id, outcome_cohort_id) as cohort_definition_id
into #cohort_details
from

(
select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnO' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'OnT' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnOc' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
0 as outcome_cohort_id,
'T' as cohort_type
from (select distinct cohort_definition_id from #targets_agg) as t

union

select distinct
0 as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'O' as cohort_type
from (select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TiOT' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TiOO' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'OiOT' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'OiOO' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
0 as outcome_cohort_id,
'Tf' as cohort_type
from (select distinct cohort_definition_id from #targets_agg) as t

union

select distinct
0 as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'Of' as cohort_type
from (select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
0 as outcome_cohort_id,
'Tl' as cohort_type
from (select distinct cohort_definition_id from #targets_agg) as t

union

select distinct
0 as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'Ol' as cohort_type
from (select distinct cohort_definition_id from #outcomes_agg) as o

) temp;


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
drop table if exists #target_overlaps_outcome;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_overlaps_outcome
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- target starts before outcome end
t.cohort_start_date <= o.cohort_end_date
and
-- outcome starts after target start
t.cohort_end_date >= o.cohort_start_date;

-- 4) get all the events where outcome overlaps target
drop table if exists #outcome_overlaps_target;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #outcome_overlaps_target
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- outcomes starts before target end
o.cohort_start_date <= o.cohort_end_date
and
-- target starts after outcome start
o.cohort_end_date >= t.cohort_start_date;

-- Final: select into #agg_cohorts

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

union

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

union

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

union

-- Ts and Os

select distinct * from (

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_agg as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'T'

union

select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'O'

union

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

union

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

union

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

union

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
) temp_ts

union

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #target_overlaps_outcome t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'TiOT'

union

select
t.subject_id,
t.outcome_start_date,
t.outcome_end_date,
cd.cohort_definition_id
from #target_overlaps_outcome t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'TiOO'

union

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #outcome_overlaps_target t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'OiTT'

union

select
t.subject_id,
t.outcome_start_date,
t.outcome_end_date,
cd.cohort_definition_id
from #outcome_overlaps_target t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.target_cohort_id
and cd.outcome_cohort_id = t.outcome_cohort_id
and cd.cohort_type = 'OiTO'

) temp_ts2;
