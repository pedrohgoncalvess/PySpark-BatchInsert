from __init__ import initSparkSession as session, initDataframe
from Treatment import treatmentEstabelecimentos
from pyspark.sql import functions as f

from types import FunctionType


def initView(func: FunctionType) -> FunctionType:
    estabelecimentos = treatmentEstabelecimentos()
    estabelecimentos.createOrReplaceTempView("empresasView")
    def viewVisu(query):
        print("SQL Query view")
        func(query)

    return viewVisu


@initView
def viewOne(query: str):
    return session().sql(query).show(5)

viewOne('select * from empresasView')





