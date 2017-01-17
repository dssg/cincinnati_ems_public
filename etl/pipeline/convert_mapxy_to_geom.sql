-- First, make a new `geom` column for QGIS
alter table
    @TABLE drop
        column if exists geom;
alter table
    @TABLE add column geom geometry(Point, 4326);
update
    @TABLE
set
    geom = ST_Transform(ST_SetSRID(ST_Point(i_mapx, i_mapy), 3735), 4326);

-- Now do the same for longitude and latitude using the geom column
alter table
    @TABLE drop
        column if exists longitude;
alter table
    @TABLE add column longitude double precision;
update
    @TABLE
    set
        longitude = st_y(geom);

alter table
    @TABLE drop
        column if exists latitude;
alter table
    @TABLE add column latitude double precision;
update
    @TABLE
    set
        latitude = st_x(geom);
