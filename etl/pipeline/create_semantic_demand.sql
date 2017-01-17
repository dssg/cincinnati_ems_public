drop schema if exists semantic_demand cascade;
create schema semantic_demand;

create temp table agg_incidents as (
select 
	date_trunc('hour', time_created) as time_created,
	station_name,
	count(incident) as total_incidents,
	sum(trns_to_hosp::int) as trns_to_hosp,
	sum(m_required::int) as m_required,
	sum(m_sent::int) as m_sent
from semantic.master
where station_name is not null
group by date_trunc('hour', time_created),station_name);

create temp table stations as (
select areas.battalion as station_name
from luigi_raw_shp.cad_station_areas as areas
group by station_name);

create temp table all_rows as (
select time_created,station_name from generate_series('2011-01-01 00:00'::timestamp,
                              '2015-12-31 23:00', '1 hour') as time_created,stations); 

create table semantic_demand.master as (
select time_created::text || '_' ||station_name as pk_demand,
time_created,
station_name,
extract (year from time_created) as time_year,
extract (month from time_created) as time_month,
extract (day from time_created) as time_day,
extract (hour from time_created) as time_hour,
coalesce(total_incidents, 0) as total_incidents,
coalesce(trns_to_hosp, 0) as trns_to_hosp,
coalesce(m_required, 0) as m_required,
coalesce(m_sent, 0) as m_sent
from all_rows
left join agg_incidents agg using (time_created, station_name));

