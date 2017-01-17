# coding: utf-8
import os
import errno
import luigi
import luigi.contrib
import luigi.postgres

from utils import pg_tools
from etl.table_info import data_paths, shp_info, tiger_urls
from incident_cleaning import RawDataToPG

import logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati-shapefiles')


def mkdir_p(path):
    """ If a directory does not exist, create it. """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: 
            raise


def make_default_profile(filename, state, state_abbrev):
    postgres_dict = pg_tools.get_pgdict_from_cfg()
    with open(filename, 'wb') as f:
        for k, v in postgres_dict.iteritems():
            f.write("{}={}\n".format(k, v))
        f.write("STATE={}\n".format(state))
        f.write("STATE_ABBREV={}\n".format(state_abbrev))


class CloneAGithubRepo(pg_tools.PostgresTask):
    repo_url = luigi.Parameter()
    folder = luigi.Parameter()

    def dirname(self):
        return self.folder + "/" + os.path.basename(self.repo_url)[:-4]

    def output(self):
        return luigi.LocalTarget(self.dirname() + '/.git')
    
    def run(self):
        # Make the folder if it doesn't exist
        mkdir_p(self.folder)
        self.pgw.shell('git clone {} {}'.format(self.repo_url, self.dirname()))

class RunDrakeForACS(pg_tools.PostgresTask):

    acs2postgres_dir = '/mnt/data/cincinnati/acs2postgres/'
    dp_name = 'default_profile'
    state = 'Ohio'
    abbrev = 'oh'

    def requires(self):
        return CloneAGithubRepo(repo_url='https://github.com/dssg/acs2postgres.git',
            folder='/mnt/data/cincinnati/')
    
    def output(self):
        yield pg_tools.PGTableTarget(schema='acs2009_5yr', table='geoheader')
        yield pg_tools.PGTableTarget(schema='acs2010_5yr', table='geoheader')
        yield pg_tools.PGTableTarget(schema='acs2011_5yr', table='geoheader')
        yield pg_tools.PGTableTarget(schema='acs2012_5yr', table='geoheader')
        yield pg_tools.PGTableTarget(schema='acs2013_5yr', table='geoheader')
        yield pg_tools.PGTableTarget(schema='acs2014_5yr', table='geoheader')
    
    def run(self):
        make_default_profile(filename=self.acs2postgres_dir + self.dp_name, 
                             state=self.state, 
                             state_abbrev=self.abbrev)
        self.pgw.shell('drake -a -w {} > {}drake_output.log'.format(self.acs2postgres_dir, 
						    		    self.acs2postgres_dir))
        self.pgw.shell('rm {}{}'.format(self.acs2postgres_dir, self.dp_name))
