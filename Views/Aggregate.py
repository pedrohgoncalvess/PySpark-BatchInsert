from __init__ import initDataframe, initSparkSession
from pyspark.sql import functions as f

from types import FunctionType


estabelecimentos = initDataframe(initSparkSession())





def view(func:FunctionType):
    estabelecimentos.select("nome_fantasia").summary().show()

    def viewVisu():
        print(func())

    return viewVisu()

@view
def viewDtCadast():
    estabelecimentos.select(f.year('data_situacao_cadastral').alias('ano')).\
        where("ano >= '2010'").\
        groupBy('ano').\
        count().orderBy('ano',ascending=True).show()    #TEM QUE USAR O ALIAS PARA PASSAR O GROUPBY E O ORDERBY


