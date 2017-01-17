# coding: utf-8
import os
import urllib
import zipfile
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
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class RawShapefilesToPG(RawDataToPG):
    """
        Imports all shape files in the data_paths['shp'] dictionary
    """

    schema = 'shp'
    table = luigi.Parameter()

    def get_shp_file(self):
        """
        This helper function just uses the targetpath to extract the shapename
        """
        files = os.listdir(self.targetpath)
        file = files[0].split('.')[0]
        return self.targetpath + '/' + file

    def output(self):
        return pg_tools.PGTableTarget(schema='luigi_raw_' + self.schema,
                                      table=self.table)

    def run(self):
        # Build up the shp2pgsql command
        command = 'shp2pgsql -I '

        # If there is a projection transformation in the shp_info dictionary
        if (shp_info[self.table][0] > 0) and (shp_info[self.table][1] > 0):
            command = command + '-s {}:{} '.format(shp_info[self.table][0],
                                                   shp_info[self.table][1])
        # now submit the rest
        command = command + '-d {} {}.{} | psql'.format(self.get_shp_file(),
                                                        'luigi_raw_' +
                                                        self.schema,
                                                        self.table)

        self.pgw.shell(command)


class DownloadTIGER(luigi.Task):
    """
        Class designed for downloading and unzipping TIGER zip files
    """
    url = luigi.Parameter()
    folder = luigi.Parameter()

    def filename(self):
        """
            Returns the full path of the final (uncompressed) shapefile
        """
        _, tail = os.path.split(self.url)
        return self.folder + '/' + tail[:-4] + '/' + tail[:-3] + 'shp'

    def zipname(self):
        """
            Returns the path of the local (downloaded) zip file
        """
        _, tail = os.path.split(self.url)
        return self.folder + '/' + tail

    def dirname(self):
        """
            Returns the folder path for unzip location (same name as zipfile)
        """
        _, tail = os.path.split(self.url)
        return self.folder + '/' + tail[:-4]

    def output(self):
        return luigi.LocalTarget(self.filename())

    def run(self):
        # Make the folder if it doesn't exist
        mkdir_p(self.folder)

        # Download to the tempfolder
        downloader = urllib.URLopener()
        downloader.retrieve(url=self.url, filename=self.zipname())

        # Extract into a subfolder with same name as zip file
        zipper = zipfile.ZipFile(self.zipname())
        zipper.extractall(self.dirname())


class TIGERToPG(pg_tools.PostgresTask):
    """
        Inserts TIGER data into Postgres.
    """
    schema = 'tiger_shp'
    folder = luigi.Parameter()
    url = luigi.Parameter()

    def tablename(self):
        """
            Takes the URL and extracts the table name based on filename
        """
        _, tail = os.path.split(self.url)
        return tail[:-4]

    def shpname(self):
        """
            Returns the shpfile info in the format shp2pgsql needs
        """
        _, tail = os.path.split(self.url)
        return self.folder + ('/' + tail[:-4]) * 2

    def run(self):
        """
            Returns the full path of the final (uncompressed) shapefile
        """
        command = ("shp2pgsql -I -s 4326 -d {} {}.{}|psql").format(self.shpname(),
                                                                self.schema,
                                                                self.tablename())

        self.pgw.shell(command)

    def output(self):
        return pg_tools.PGTableTarget(schema=self.schema,
                                      table=self.tablename())

    def requires(self):
        return [DownloadTIGER(url=self.url, folder=self.folder),
                pg_tools.CreateSchema(schema=self.schema)]


class ImportingShapefiles(luigi.WrapperTask):
    """
        WrapperTask that will loop through all shapefiles and import
    """

    def requires(self):
        to_return = []
        for path in data_paths['shp']:
            table = path.split('/')[-1]
            to_return.append(RawShapefilesToPG(targetpath=path, table=table))
        return to_return


class ImportingTIGERFiles(luigi.WrapperTask):
    """
        WrapperTask loops through TIGER urls
    """

    folder = '/mnt/data/cincinnati/shp_temp'

    def requires(self):
        return [TIGERToPG(url=url, folder=self.folder) for url in tiger_urls]
