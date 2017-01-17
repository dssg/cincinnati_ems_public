# coding: utf-8
import  logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati')

import luigi

from utils import pg_tools
from create_semantic import CreateSemanticTable

class CreateSemanticDemandTable(pg_tools.PostgresTask):
  """
  runs set of SQL queries that create the semantic demand table
  "master" in the schema "semantic_demand"

  communication with the DB is done with PostgresTask
  """

  schema = luigi.Parameter()
  table = luigi.Parameter()
  sql_file = luigi.Parameter()

  def requires(self):
    return [CreateSemanticTable()]

  def output(self):
    return pg_tools.PGTableTarget(schema=self.schema, table=self.table)

  def run(self):
    self.pgw.shell("psql -f %s"%self.sql_file)

 
