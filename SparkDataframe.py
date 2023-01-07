from Treatment import treatmentEstabelecimentos
from types import FunctionType
from typing import Tuple
from pandas.core.frame import DataFrame
from pandas.core.series import Series




def dataframe(index_inicio: int, index_final:int) -> Tuple:

    estabelecimentos = treatmentEstabelecimentos().select('cnpj_basico','nome_fantasia','bairro','uf','municipio')
    df = estabelecimentos.toPandas()
    df = df.iloc[index_inicio:index_final]
    tupla = list(df.itertuples(index=False,name=None))

    return tupla


estabelecimentos = treatmentEstabelecimentos().select('cnpj_basico', 'nome_fantasia', 'bairro', 'uf', 'municipio')
df = estabelecimentos.toPandas()
print(type(df['uf']))


def iterador(dataframe: DataFrame, column:Series):
    count = dataframe[column].count()
    print(count)


iterador(df,'uf')


