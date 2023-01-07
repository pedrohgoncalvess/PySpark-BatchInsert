from InitSpark import initSparkSession as session, initDataframe
from Treatment import treatmentEstabelecimentos
from pyspark.sql import functions as f

from types import FunctionType


def initView(func: FunctionType) -> FunctionType:
    estabelecimentos = treatmentEstabelecimentos()
    estabelecimentos.createOrReplaceTempView("empresasView")
    def viewVisu():
        print("SQL Query view")
        func()

    return viewVisu


@initView
def viewOne():
    query = "select * from empresasview"
    return session().sql(query).show(5)

viewOne()




