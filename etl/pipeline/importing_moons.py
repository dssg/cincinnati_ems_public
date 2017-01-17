# coding: utf-8

import luigi
import os
import datetime
import luigi.postgres
from luigi import configuration

from utils import pg_tools

import logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati')

class SuccessFile(luigi.ExternalTask):
    filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filename + '.success')


class CSVToPG(pg_tools.PostgresTask):
    """
    Imports raw csv files into Postgres

    """

    schema = luigi.Parameter()
    table = luigi.Parameter()
    filename = luigi.Parameter()

    def requires(self):
        # Check if the file exists
        return pg_tools.RawData(self.filename)

    def make_success_file(self):
        with open(self.filename + '.success', 'wb') as f:
            f.write("{}".format(datetime.datetime.now()))

    def run(self):
        self.pgw.execute('create schema if not exists {}'.format(self.schema))

        # Make the headers
        command = "head -n 100 {} | ".format(self.filename)
        command = command + "iconv -t ascii | csvsql -i postgresql"
        
        headers = self.pgw.shell(command)
        headers = headers.replace('stdin', 'if not exists {}.{}'.format(self.schema, 
                                                          self.table))
        headers = headers.replace("\t", "").replace("\n", "")
        self.pgw.execute(headers)
        
        # Import the data
        command = 'cat {} | psql -c "\copy {}.{} '.format(self.filename, 
                                                          self.schema, 
                                                          self.table)
        command = command + 'from stdin with csv header;"'
        self.pgw.shell(command)
        
        # Create success file
        self.make_success_file()

    def output(self):
        yield luigi.LocalTarget(self.filename + '.success') 

class MoonsToPG(luigi.WrapperTask):
    folder = luigi.Parameter()

    def requires(self):
        files = [f for f in os.listdir(self.folder) if f.endswith('.csv')]        
        for file in files:
            yield CSVToPG(filename=self.folder + file, 
                        schema='external', table='moon')
