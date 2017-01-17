from utils.pg_tools import PostgresTask, PostgresTarget, PGColumnTarget
from utils.pg_tools import CreateSchema, PGTableTarget, PGNColumnsTarget
from utils.pg_tools import DropTable, FileOlderTarget 
from etl.pipeline.create_semantic import CreateSemanticTable
import luigi
from luigi import configuration
import pandas as pd
import yaml

from features import *

class CreateFeatureTable(PostgresTask):
    """
    Wrapper task for feature creation

    :param str feature_table: table for features
    :param str feature_schema: schema
    :param str data_table: table to pull raw features from
    :param str data_schema: schema
    """
    feature_table = luigi.Parameter()
    feature_schema = luigi.Parameter()
    data_table = luigi.Parameter()
    data_schema = luigi.Parameter()
    success_file = luigi.Parameter()
    specs_file = luigi.Parameter()

    def load_specs(self):
        """
        Loads model specification from yaml file

        :returns: the primary key, a list of features, list of raw dependencies
        :rtype: (str, [str], [str])
        """
        with open(self.specs_file) as stream:
            specs = yaml.load(stream)
        pk = specs['pk']
        tmp_features = set()
        features = []
        for feature in specs['features']:
            if feature['incl']:
                features.append((feature['name'], feature['type']))
                try:
                    tmp_features |= set(feature['deps'])
                except:
                    # No dependencies
                    pass
        # Some dependencies might actually be features
        return (pk, features, list(tmp_features))

    def requires(self):
        return [CreateSemanticTable(),
                CreateSchema(self.feature_schema),
                DropTable(table=self.feature_table,
                        schema=self.feature_schema),
                DropTable(table=self.feature_table + '_types',
                        schema=self.feature_schema)]
 
    def run(self):
        pk, features, tmp_features = self.load_specs()
        function_features = {}
        semantic_features = []
        # First find those features, that don't have functions
        glob = globals()
        for feature, _ in features:
            func_str = feature + '_feature'
            if func_str in glob:
                function_features[feature] = glob[func_str]
            # This assumes that all features that don't have functions are
            # in the semantic table
            else:
                semantic_features.append(feature)

        # Read all non-function features from semantic maaster
        p = {'schema':self.data_schema,
             'table':self.data_table,
             # Also read the tmp features needed for feature generation
             # tmp_featuers and semantic_features might overlap
             'cols':",".join(['"%s"'%x for x in list(set(semantic_features + 
                              tmp_features + [pk]))])
            }
        # Using PGDB here would remove types (like datetime) because it goes 
        # through csvs
        conn = pgw.get_conn()
        df = pd.read_sql_query("select {p[cols]} from "
                                 "{p[schema]}.{p[table]}".format(p=p),
                                 conn)
        conn.close()
        df.set_index(pk, inplace=True)

        # Apply all function features
        def feature_type(feature):
            return [y for (x, y) in features if x == feature][0]
        feature_types = []
        # Go through funciton features in ordered way
        for f in sorted(function_features.keys()):
            newdata = function_features[f](df)
            ftype = feature_type(f)
            try:
                df[f] = newdata
                feature_types.append((f, ftype))
            except:
                for col in newdata:
                    f_name = f + '_' + col
                    df[f_name] = newdata[col]
                    feature_types.append((f_name, ftype))

        for f in semantic_features:
            feature_types.append((f, feature_type(f)))


        # Drop all temporary features
        feature_names = [x for (x,_) in features]
        for f in tmp_features:
            if not f in feature_names:
                df.drop(f, axis=1, inplace=True)

        # Save in sql
        self.pgw.df_to_pg(df, self.feature_table, self.feature_schema, 
                          pk=pk)

        # Save feature types
        type_df = pd.DataFrame(feature_types, columns=['name', 'type'])
        type_df.set_index('name', inplace=True)
        self.pgw.df_to_pg(type_df, self.feature_table + '_types',
                          self.feature_schema, pk='name')

        self.pgw.create_timestamp(self.success_file)

    def output(self):
        yield FileOlderTarget(old_file=self.specs_file,
                              young_file=self.success_file)
        yield PGTableTarget(table=self.feature_table, 
                            schema=self.feature_schema)
        yield PGTableTarget(table=self.feature_table + '_types',
                            schema=self.feature_schema)
