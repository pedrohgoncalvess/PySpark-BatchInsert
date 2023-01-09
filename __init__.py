import os

import pyspark.sql.dataframe
from pyspark.sql.session import SparkSession
from pandas.core.frame import DataFrame
from dotenv import load_dotenv
from typing import Dict


load_dotenv()

def environment_variables(variable:str) -> str:
    var = os.getenv(variable)

    return var



def initSparkSession() -> SparkSession:
    import os

    os.environ['SPARK_HOME'] = environment_variables('SPARK_HOME')
    os.environ['HADOOP_HOME'] = environment_variables('HADOOP_HOME')

    import findspark

    findspark.init()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master('local[*]').getOrCreate()

    return spark


def initDataframe(sparksession: SparkSession) -> pyspark.sql.dataframe.DataFrame:

    path = environment_variables('path_arq')
    estabelecimentos = sparksession.read.csv(path,sep=';',inferSchema=True)

    return estabelecimentos

