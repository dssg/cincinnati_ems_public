import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.pgtools import PGWrangler

# Import table info
from etl.table_info import joins_CAD, joins_SP
pgw = PGWrangler()

def join_tables(table_list, from_schema, to_schema, cond=None):
    """ Iteratively left-joins a set of tables
    
    This assumes that the tables are given in an order that the later tables
    can be linked to the earlier tables through the given key pair
    
    :param [((str,str),(str,str))] table_list: list of pairs of tables and the 
                                               keys that link the tables
    :param str from_schema: schema from where the tables to be joint originate
    :param str to_schema: schema to copy the joint table to
    :param (str,(str,str)) cond: name and (min,max) pair of column in left 
                                 table to condition on
    
    """
    # Iteratively create tables joined_table_0, joined_table_1 with the 
    # joined tables
    JOINED_TABLE = 'joined_table'
    JOINED_SCHEMA = to_schema
    base_table_left = table_list[0][0][0]
    table_left = base_table_left
    new_joined_table = JOINED_TABLE
    left_schema = from_schema
    condition=cond

    for (counter,((table_left_dict, table_right),
                  (key_left, key_right))) in enumerate(table_list):
        if not table_left_dict == base_table_left:
            key_left = table_left_dict + '__' + key_left
        pgw.left_join(table_left,table_right,key_left,key_right,left_schema,
                  from_schema,JOINED_SCHEMA+'.'+new_joined_table,
                  cond=condition)
        
        # drop intermediate tables
        if not table_left is base_table_left:
            pgw.drop_table(table_left,left_schema)
        else:
            # Only have to subset for first left join
            condition=None

        # For the next iteration, the new left table will be the joined table 
        # so far
        table_left = new_joined_table
        new_joined_table = JOINED_TABLE + "_%d"%counter
        left_schema = JOINED_SCHEMA   
    return table_left

def join_tables_date_range(start_date,end_date,all_sp=False):
    # Join all CAD tables
    CAD_cond = ('i_ttimecreate',(start_date,end_date))
    SP_cond = ('CREATEDATETIME',(start_date,end_date))
    CAD_joined_table = join_tables(joins_CAD, 'clean_csvs', 'clean_csvs',
                                   cond=CAD_cond)
    
    # Add some safety pad tables
    join_name=("join_%s_%s"%(start_date,end_date)).replace("-","_")
    
    if all_sp:
        join_name += "_all_sp"
        SP_joined_table = join_tables(joins_SP,'clean_safety_pad',
                                          'clean_safety_pad',
                                          cond=SP_cond)
        pgw.left_join(CAD_joined_table,SP_joined_table,
                      "i_eventnumber", "INCIDENTN",
                      "clean_csvs", "clean_safety_pad", "clean.tmp3")
        pgw.drop_table(SP_joined_table, 'clean_safety_pad')
    else:
        pgw.left_join(CAD_joined_table, 't_incident', 
                      "i_eventnumber", "INCIDENTN", 
                      "clean_csvs", "clean_safety_pad", "clean.tmp")
        pgw.left_join('tmp', 't_case', "t_incident__KEY_INCIDENT", 
                      "KEY_INCIDENT", "clean", "clean_safety_pad", 
                      "clean.tmp2")
        pgw.left_join('tmp2', 't_destination', "t_case__KEY_CASE", "KEY_CASE", 
                  "clean", "clean_safety_pad", "clean.tmp3")
        pgw.drop_table('tmp','clean')
        pgw.drop_table('tmp2','clean')

    # Add the response dictionary
    pgw.left_join('tmp3','cdf_response_typ', 
              'dbo_rfirehouseincident__iti_typeid', "code", 
              'clean', "clean","clean."+join_name)

    # Clean up
    pgw.drop_table('tmp3','clean')
    pgw.drop_table(CAD_joined_table,'clean_csvs')

if __name__=='__main__':
    start_date = '2015-10-11'
    end_date = '2015-10-14'
    join_tables_date_range(start_date,end_date,all_sp=True)
