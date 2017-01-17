# coding: utf-8

import luigi
import luigi.configuration as configuration
import luigi.contrib
import luigi.postgres

import  logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati')

from utils import pg_tools
from incident_cleaning import IncidentCleaning
from importing_shapefiles import ImportingShapefiles, ImportingTIGERFiles 
from importing_moons import MoonsToPG
from importing_weather import WeatherToPG
from importing_acs_data import RunDrakeForACS

class CreateSemanticTable(pg_tools.PostgresTask):
  """
  runs set of SQL queries that create the semantic table
  "master" in the schema "semantic"

  communication with the DB is done with PostgresTask
  """

  schema = luigi.Parameter()
  table = luigi.Parameter()
  sql_file = luigi.Parameter()

  def requires(self):
    return [IncidentCleaning(), ImportingShapefiles(), ImportingTIGERFiles(), MoonsToPG(), WeatherToPG(), RunDrakeForACS()]

  def output(self):
    return pg_tools.PGTableTarget(schema=self.schema, table=self.table)

  def run(self):
    self.pgw.shell("psql -f %s"%self.sql_file)

