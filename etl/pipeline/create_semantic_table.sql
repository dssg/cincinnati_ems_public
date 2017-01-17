-- erase all tables from previous run
drop schema if exists semantic cascade;
create schema semantic;

-- disposition codes of transportation
create temp table trns_disp_codes as
(select *,
case when idi_dispositioncode like '%T'
	and idi_dispositiontext not like '%NO%'
	and idi_dispositioncode not like 'TEST'
	and idi_dispositioncode not like 'AST'
	and idi_dispositioncode not like 'CIT'
	and idi_dispositioncode not like 'EXT'
	and idi_dispositioncode not like 'NOT'
	or (idi_dispositiontext like '%TRANSPORT%'
		and idi_dispositiontext not like '%NO%')
	or idi_dispositioncode like 'TRAN%'
	or idi_dispositioncode like 'NCT'
then true else false end as trns_to_hosp_temp
from luigi_clean_cad.udt4_idisposition
);

create temp table distinct_events as
(select distinct incidents.i_eventnumber, 
	         incidents.i_ttimecreate, 
                 incidents.i_ttimeclosed, 
                 incidents.i_kcallsource,
                 incidents.i_callerphonetype,
                 incidents.geom,
		 case when trns_to_hosp_temp is null then false else trns_to_hosp_temp end as trns_to_hosp
from luigi_clean_cad.dbo_iincident incidents
left join trns_disp_codes disp on
	disp.idi_disposition_pk = incidents.i_kdisposition1
where incidents.i_ttimecreate >= '2011-01-01');
alter table distinct_events add primary key (i_eventnumber);
create index incident_geom_gix on distinct_events using GIST (geom);

--events where a transportation unit was sent
create temp table events_with_mt as (
select distinct i_eventnumber
from luigi_clean_cad.dbo_rfirehouseapparatus
where pun_unitid like 'M__'
	or pun_unitid like 'A__'
	or pun_unitid like 'R__' );

	
-- few duplicates - irrelevant for our rows (dispcode&text)
create temp table corrected_code AS (
select distinct on (i_eventnumber) * from 
luigi_clean_cad.dbo_rfirehouseincident);

update corrected_code
set iti_typeid = RIGHT(iti_typeid, LENGTH(iti_typeid) - 1)
where iti_typeid like '0%' ;
create index on corrected_code (i_eventnumber);

-- create timestamps table
create temp table timestamps as (
select 
	i_eventnumber, 
	i_ttimecreate,
	extract(year from i_ttimecreate) as time_year, 
	extract(month from i_ttimecreate) as time_month, 
	extract(day from i_ttimecreate) as time_day,
	extract(hour from i_ttimecreate) as time_hour
from distinct_events);
alter table  timestamps add primary key  (i_eventnumber);

--create codes table
create temp table splitcodes as (
select 
    i_eventnumber,
    iti_typeid,
    coalesce(to_char(substring(iti_typeid, '^(\d+)[A-Z]+.*')::integer,'FM99'),
             iti_typeid) as code_type,
    substring(iti_typeid, '^\d+([A-Z]+).*') as code_level,
    substring(iti_typeid, '^\d+[A-Z]+(.*)') as code_rest
from corrected_code);
create index on splitcodes  (i_eventnumber);


create temp table geoid as (
select incident.i_eventnumber, tiger.geoid as geoid
from distinct_events as incident,
	tiger_shp.tl_2013_39_bg as tiger
where ST_WITHIN(incident.geom, tiger.geom));
alter table geoid add primary key  (i_eventnumber);

create temp table station_area as (
select incident.i_eventnumber, 
	areas.geom as cfd_geom,
	areas.battalion as station_name
from distinct_events as incident,
	luigi_raw_shp.cad_station_areas as areas
where ST_WITHIN(incident.geom, areas.geom));
alter table station_area add primary key  (i_eventnumber);

-- get operator name and dispatch comments
create temp table disp_comments as (
select i_eventnumber,ico_comment,poc_operatorname 
from luigi_clean_cad.dbo_rfirehouseincident
group by 1,2,3);

create temp table comments as (
select events.i_eventnumber,
      string_agg(comm."COMMENTTEXT", ' | ') as comments
from  distinct_events as events,
      luigi_clean_sp.t_incident as inc,
      luigi_clean_sp.t_case as c,
      luigi_clean_sp.t_comment as comm
where events.i_eventnumber = inc."INCIDENTN"
      and inc."KEY_INCIDENT" = c."KEY_INCIDENT"
      and c."KEY_CASE" = comm."KEY_CASE"
group by events.i_eventnumber);

create temp table patient_details as (
select events.i_eventnumber,
  	max(pt."GENDERCODE") pt_gender,
        max(pt."AGE") as pt_age,
        max(pt."RACECODE") as pt_race,
	max(pt."POSTCODE") as pt_postcode
from distinct_events as events,
	luigi_clean_sp.t_incident as inc,
	luigi_clean_sp.t_case as c,
	luigi_clean_sp.t_patient as pt 
where events.i_eventnumber = inc."INCIDENTN" 
	and inc."KEY_INCIDENT" = c."KEY_INCIDENT"
	and c."KEY_PATIENT" = pt."KEY_PATIENT"   
group by i_eventnumber
);

-- create master table
create table semantic.master as (
select events.i_eventnumber incident, 
   events.i_ttimecreate time_created, 
   events.i_ttimeclosed time_closed, 
   ts.time_year,
   ts.time_month,
   ts.time_day,
   ts.time_hour,
   events.i_kcallsource call_source,
   events.i_callerphonetype call_type,
   dsp_cm.poc_operatorname operator_name,
   dsp_cm.ico_comment dispatch_comment,
   other_incidents.iti_typeid dispatch_code, 
   other_incidents.iti_typetext dispatch_text, 
   dict.incident_type dispatch_type,
   sc.code_type,
   sc.code_level,
   sc.code_rest,
   events.geom geom,
   geoid.geoid,
   station.cfd_geom,
   station.station_name,
   cm.comments,
   events.trns_to_hosp,
   pt.pt_gender,
   pt.pt_age,
   pt.pt_race,
   pt.pt_postcode,
case when dsp_cm.ico_comment like '%CP/%' then 'CP' 
     when dsp_cm.ico_comment like '%CF/%' then 'CF'
     else 'NA' end as operator_agency,
case when mt.i_eventnumber is null then false else true end as m_sent,
case when dict.compliment like '%MT%' then true else false end as m_required
from distinct_events events
left join events_with_mt mt using (i_eventnumber)
left join corrected_code other_incidents using(i_eventnumber)
left join luigi_clean_info.cdf_response_typ dict on dict.code = other_incidents.iti_typeid
left join timestamps ts using (i_eventnumber)
left join splitcodes sc using (i_eventnumber)
left join geoid geoid using (i_eventnumber)
left join station_area station using (i_eventnumber)
left join comments cm using (i_eventnumber)
left join patient_details pt using (i_eventnumber)
left join disp_comments dsp_cm using (i_eventnumber)
where other_incidents.iti_typeid not like 'INFOF'
and other_incidents.iti_typeid not like 'FADV'
and other_incidents.iti_typeid not like 'FTEST'
and other_incidents.iti_typeid not like 'TESTF'
and other_incidents.iti_typeid not like 'TESTC'
and other_incidents.iti_typeid not like 'DETAIL'
and other_incidents.iti_typeid not like 'FDRILL'
and other_incidents.iti_typeid not like 'FSERV'
and other_incidents.iti_typeid is not null
);

