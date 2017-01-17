from utils.pg_tools import PostgresTask, PostgresTarget, PGColumnTarget
from utils.pg_tools import CreateSchema, PGTableTarget, PGNColumnsTarget
from utils.pg_tools import DropTable, FileOlderTarget 
from etl.pipeline.create_semantic import CreateSemanticTable
import luigi
from luigi import configuration
import pandas as pd
import yaml

from feature_creation import CreateFeatureTable
from create_semantic_demand import CreateSemanticDemandTable

from features import *

class CreateDemandFeatureTable(CreateFeatureTable):
    """
    Wrapper task for demand feature creation
    """
    def requires(self):
        supertasks = super(CreateDemandFeatureTable, self).requires()
        return supertasks + [CreateSemanticDemandTable()]
