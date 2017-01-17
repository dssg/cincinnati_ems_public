def get_multiplier_table():
    """
    By mkiang

    for aggregating acs-features to station-level
    """
    import pandas as pd
    import utils.pg_tools as pg

    pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())
    conn = pgw.get_conn()

    multiplier = pd.read_sql_query('select a.battalion as station_name, b.geoid, '
                                'st_area(st_intersection(a.geom, b.geom)) / '
                                'st_area(b.geom) as multiplier '
                                'from tiger_shp.tl_2013_39_bg as b '
                                'join luigi_raw_shp.cad_station_areas as a on '
                                'st_intersects(a.geom, b.geom)', conn)

    conn.close()

    multiplier.set_index('geoid', inplace=True, drop=True)

    return multiplier
