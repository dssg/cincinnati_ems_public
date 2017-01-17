# coding: utf-8
import sys
import os

import luigi
import luigi.postgres
from luigi import configuration

import logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati')

import subprocess
import re

from utils.pg_tools import CreateSchema, RawData, CastColumn
from utils.pg_tools import PGWrangler, PostgresTask, PostgresTarget
from utils.pg_tools import PGSchemaTarget, PGTableTarget, PGColValTarget
from utils.pg_tools import PGColumnTarget, DeleteDuplicates
from etl.table_info import data_paths, tables, datetime_columns


class IncidentCleaning(luigi.WrapperTask):
    """ Wrapper task for all incident importing and cleaning

    """

    def requires(self):
        # tables is the imported hand-coded schema-table mapping
        for schema in tables:
            for table in tables[schema]:
                yield CleanTable(table, schema)
        yield FixIndices('INCIDENTN', 'T_INCIDENT', 'sp')
        yield FixIndices('INCIDENTN', 'dbo_T_INCIDENT', 'sp')
        yield CreateGeom('dbo_iincident', 'cad')
        yield DeleteDuplicates('response_type_id', 'cdf_response_typ',
                               'luigi_clean_info', 
                               required=[CleanTable('cdf_response_typ',
                                                    'info')])

def clean_table_name(filename):
    """ Cleans a filename to a tablename

    :param str filename: name of file

    """
    return "_".join(filename.split('/')[-1].split('.')[:-1]).lower()

class FixIndices(PostgresTask):
    """ Fixes indices

    :param str column: column to be fixed
    :param str table: table
    :param str schema: schema

    """
    column = luigi.Parameter()
    table = luigi.Parameter()
    schema = luigi.Parameter()

    def requires(self):
        return CleanTable(self.table,self.schema)

    def run(self):
        table = self.table.lower()
        schema = 'luigi_clean_'+self.schema

        p = {'table':schema+'.'+table,'col':'"%s"'%self.column}
        self.pgw.execute("update {p[table]} set {p[col]} = replace({p[col]}, "
                          "'O', '0')".format(p=p))
        self.pgw.execute("alter table {p[table]} alter column {p[col]} type "
                          "bigint using {p[col]}::bigint".format(p=p))
        self.pgw.execute("alter table {p[table]} alter column {p[col]} type "
                          "text using to_char({p[col]},"
                          "'\"FCF\"000000000009')".format(p=p))
        # The previous command creates a space between FCF and number
        # -> remove it
        self.pgw.execute("update {p[table]} set {p[col]} = replace({p[col]}, "
                          "' ', '')".format(p=p))

    def output(self):
        return PGColValTarget(lambda x: x[:3]=='FCF',
                              'INCIDENTN',self.table.lower(),
                              'luigi_clean_'+self.schema)


class CreateGeom(PostgresTask):
    """
    Creates a geom with the convert...sql script

    :param str table: table
    :param str schema: schema

    """
    table = luigi.Parameter()
    schema = luigi.Parameter()

    def requires(self):
        ct = CleanTable(self.table, self.schema)
        return CastColumn('integer', 'i_mapy', self.table,
                         'luigi_clean_' + self.schema,
                         required_tasks=[ct])

    def run(self):
        script = "convert_mapxy_to_geom.sql"
        cmd = ("sed s/@TABLE/%s/ %s | "
               "psql" % ('luigi_clean_' + self.schema + '.' + self.table, script))
        self.pgw.shell(cmd)

    def output(self):
        schema = 'luigi_clean_' + self.schema
        yield PGColumnTarget('geom', self.table, schema)
        yield PGColumnTarget('longitude', self.table, schema)
        yield PGColumnTarget('latitude', self.table, schema)


class RawDataToPG(PostgresTask):
    """ Parent class for all raw data imports

    :param str targetpath: path of file(s) to be imported
    :param str schema: schema to be imported to (w/o clean/raw)

    """
    targetpath = luigi.Parameter()
    schema = luigi.Parameter()

    def requires(self):
        return[CreateSchema('luigi_raw_' + self.schema),
               RawData(self.targetpath)]

    def run(self):
        raise NotImplementedError

    def output(self):
        raise NotImplementedError


class RawCadToPG(RawDataToPG):
    """
    Imports CAD csv files

    """
    schema = 'cad'

    def run(self):
        schema = 'luigi_raw_' + self.schema

        # Extract headers from csv with csvsql
        header_cmd = ("head -n 100000 %s | iconv -t ascii | "
                      "tr [:upper:] [:lower:] | tr ' ' '_' | "
                      "csvsql -i postgresql")
        filename = self.targetpath
        tname = clean_table_name(filename)
        table = schema + '.' + tname
        headers = self.pgw.shell(header_cmd % filename)
        headers = re.sub("VARCHAR\(.*\)", "TEXT", headers)
        headers = headers.replace('stdin', table)
        headers = headers.replace("\t", "").replace("\n", "")
        self.pgw.execute(headers)

        # Import the data
        self.pgw.shell('head -n-1 %s | psql -c "\copy %s '
                       'from stdin with csv header;"' % (filename, table))

    def output(self):
        yield PGTableTarget(clean_table_name(self.targetpath),
                            'luigi_raw_' + self.schema)


class RawSPToPG(RawDataToPG):
    """
    Imports Safety Pad files by invoking a RawSPTableToPG for each table

    """
    schema = 'sp'

    def requires(self):
        return (super(RawSPToPG, self).requires() + 
                [RawSPTableToPG(self.targetpath, table) 
                 for table in self.get_table_names()])

    def get_table_names(self):
        tables = self.pgw.shell('mdb-tables ' + self.targetpath)
        return tables.strip().split(" ")

    def run(self):
        pass

    def output(self):
        for table in self.get_table_names():
            yield PGTableTarget(table, 'luigi_raw_' + self.schema)


class RawSPTableToPG(RawSPToPG):
    """ Imports single Safety Pad tables

    """
    table = luigi.Parameter()

    def requires(self):
        return RawDataToPG.requires(self)

    def run(self):
        # use mdb-tools to create a schema and then import line-by-line
        schema_cmd = ("mdb-schema -T %s %s postgres | "
                      "sed -e 's/Postgres_Unknown 0x10/TEXT/' | "
                      'psql "dbname=$PGDATABASE options=--search_path=%s"')
        import_cmd = ("mdb-export -I postgres -q \\' %s %s | "
                      "sed -e 's/)$/)\;/' | "
                      'psql "dbname=$PGDATABASE options=--search_path=%s"'
                      ' -q')

        schema = 'luigi_raw_' + self.schema
        self.pgw.shell(schema_cmd % (self.table, self.targetpath, schema))
        self.pgw.shell(import_cmd % (self.targetpath, self.table, schema))

    def output(self):
        yield PGTableTarget(self.table, 'luigi_raw_' + self.schema)


class RawInfoToPG(RawDataToPG):
    """ TODO import all the xls dictionaries

    """
    schema = 'info'
    OUTDIR = '/mnt/data/cincinnati/processed'

    def run(self):
        if "RESPONSE_TYP" in self.targetpath:
            script = "import_dict.sh"
        elif "FIREHOUSE" in self.targetpath:
            script = "import_xls.sh"
        else:
            raise NotImplementedError
        cmd = "bash %s '%s' '%s' %s" % (script, self.targetpath,
                                        self.OUTDIR, 'luigi_raw_' + self.schema)
        self.pgw.shell(cmd)

    def output(self):
        if "RESPONSE_TYP" in self.targetpath:
            yield PGTableTarget('cdf_response_typ', 'luigi_raw_' + self.schema)
        elif "FIREHOUSE" in self.targetpath:
            yield PGTableTarget('cfd_station_info', 'luigi_raw_' + self.schema)
            yield PGTableTarget('unit_dictionary', 'luigi_raw_' + self.schema)
        else:
            raise NotImplementedError


class CleanTable(PostgresTask):
    """ Clean a table

    :param str table: the table to be cleaned
    :param str schema: the schema to be cleaned

    """
    table = luigi.Parameter()
    schema = luigi.Parameter()

    def requires(self):
        schema_to_raw = {'cad': RawCadToPG, 'sp': RawSPToPG,
                         'info': RawInfoToPG}

        return ([CreateSchema('luigi_clean_' + self.schema)] + 
                [schema_to_raw[self.schema](filename) 
                 for filename in data_paths[self.schema]])

    def run(self):
        # Copy table to clean
        self.pgw.copy_table(self.table, schema_from='luigi_raw_' + self.schema,
                            schema_to='luigi_clean_' + self.schema)
        table = self.table.lower()
        clean_schema = 'luigi_clean_' + self.schema
        # Drop Empty Columns
        self.pgw.drop_empty_columns(table, schema=clean_schema)
        # Datetime conversion
        try:
            columns = datetime_columns[self.schema][self.table]
        except:
            columns = []
        for column in columns:
            self.pgw.prettify_date(column, table, schema=clean_schema)

    def output(self):
        return PGTableTarget(self.table.lower(), 'luigi_clean_' + self.schema)

if __name__ == '__main__':
    luigi.run()
