# coding: utf-8
from utils.pg_tools import PostgresTask, PGSchemaTarget
import luigi
import logging
from gen_models import RunModels
from gen_reg_models import RunModelsReg

logger = logging.getLogger('cincinnati')

class CincinnatiEMS(luigi.WrapperTask):
    """
    Overall wrapper for the MT prediction task
    """

    def requires(self):
        return [RunModels(label='trns_to_hosp')]

class CincinnatiEMS_Demand(luigi.WrapperTask):
    """
    Overall wrapper for the demand prediction task 
    """

    def requires(self):
        return [RunModelsReg(label='trns_to_hosp')]
                
class CincinnatiClean(PostgresTask):
    base_path = luigi.Parameter()
    
    def requires(self):
        pass

    def schemas(self):
        return ['features',
                'model',
                'luigi_raw_cad',
                'luigi_raw_info',
                'luigi_raw_sp',
                'luigi_raw_shp',
                'luigi_clean_cad',
                'luigi_clean_info',
                'luigi_clean_sp',
                'luigi_clean_shp',
                'tiger_shp',
                'semantic',
                'semantic_demand',
                'features_demand',
                'external']
 

    def run(self):
        # Delete all success files
        del_cmd = 'find %s -name *.success -exec rm -f {} \;'%self.base_path
        self.pgw.shell(del_cmd)
        # Delete all PG Tables
        for schema in self.schemas():
            self.pgw.drop_schema(schema)

    def output(self):
        # TODO this does not catch all the manual deletion
        return [PGSchemaTarget(schema, inverse=True)
                for schema in self.schemas()]
