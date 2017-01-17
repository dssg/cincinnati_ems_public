-- How I got numbers for the grid plot in our presentation
-- totals -- note, I only used 2013-2015
select count(*), time_year from semantic.master group by time_year;

-- Get medics required and not required
select count(*), time_year from semantic.master
	where m_required = true group by time_year;
	
select count(*), time_year from semantic.master
	where m_required = false group by time_year;

-- Get medics misclassifications
select count(*), time_year from semantic.master
	where trns_to_hosp = false and m_required = true group by time_year;

select count(*), time_year from semantic.master
	where trns_to_hosp = true and m_required = false group by time_year;
	
-- Creating views
---------- GET INCIDENTS / RATES 
-- Now subset to just 2015
create or replace view clean_csvs.iincident2015 as
	select * from clean_csvs.dbo_iincident
		where date_part('year', i_ttimecreate) = 2015;

----------------- MAKE A NEW SCHEMA WITH QGIS STUFF
-- Create a schema
create schema if not exists mkiang;

--- Create a table of only block groups that are within
--- the polygons of Cincinnati (vs all of Hamilton county)
drop table if exists mkiang.tiger_bg;
create table mkiang.tiger_bg as(
	select t.* from raw_shp.tl_2013_39_bg as t
	where st_intersects(t.geom,
						(select st_union(geom) from raw_shp.areas))
);
commit;

-- Get the total population (b01003001) and 
-- join with the new TIGER block group table
drop table if exists mkiang.acs2014;
create table mkiang.acs2014 as (
	select t.geoid as geoid, geom, b01003001 as total_pop
		from mkiang.tiger_bg as t
	left join acs2014_5yr.b01003 as a
       on substring(a.geoid from 8) = t.geoid 
);
commit;      

--- Create a table of count of 2015 incidents per block group
drop table if exists mkiang.counts2015_all;
create table mkiang.counts2015_all as (
	select count(i_eventnumber) as incidents, geoid
		from clean_csvs.iincident2015 as i, mkiang.tiger_bg as bg
			where ST_Within(i.geom, bg.geom)
		group by geoid
); 
commit;


-- Create a new block group-level table with incidents and population
drop table if exists mkiang.acs_incidents_all;
create table mkiang.acs_incidents_all as (
	select cnt.*, acs.total_pop, acs.geom from mkiang.acs2014 as acs
		left join mkiang.counts2015_all cnt on cnt.geoid = acs.geoid
);

-- add rate of incidents per block group
alter table mkiang.acs_incidents_all add column rate real;
update mkiang.acs_incidents_all set rate = (incidents / total_pop);
commit;


---------- FIRESTATION POLYGONS
-- Make a multiplier table
drop table if exists mkiang.bg_multiplier;
create table mkiang.bg_multiplier as (
	select a.gid, b.geoid, st_area(st_intersection(a.geom, b.geom)) / st_area(b.geom) as multiplier
	from mkiang.acs_incidents_all as b
		join raw_shp.areas as a on st_intersects(a.geom, b.geom)
);
select * from mkiang.bg_multiplier;
commit;

-- Make a table that aggregates rates up to polygon level
drop table if exists mkiang.station_incidents_all;
create table mkiang.station_incidents_all as (
	select
		mult.gid,
		sum(mult.multiplier * acs.incidents) as incidents,
		sum(mult.multiplier * acs.total_pop) as pop,
		sum(mult.multiplier * acs.incidents) / sum(mult.multiplier * acs.total_pop) as rate,
		areas.geom
	from mkiang.acs_incidents_all as acs
	left join mkiang.bg_multiplier as mult
		on acs.geoid = mult.geoid
	left join raw_shp.areas as areas
		on areas.gid = mult.gid
	group by mult.gid, areas.geom
	order by gid
);
select * from mkiang.station_incidents_all limit 100;
commit;
