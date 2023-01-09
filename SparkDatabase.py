import datetime
from datetime import timedelta

from Treatment import treatmentEstabelecimentos
from Database import connect

from types import FunctionType
from typing import Tuple

from pandas.core.series import Series
from pyspark.sql.dataframe import DataFrame
import pyspark


def tupla(index_inicio: int, index_final:int, df) -> Tuple:

    df = df.iloc[index_inicio:index_final]
    tupla = list(df.itertuples(index=False,name=None))

    return tupla


def iterador(dataframe: DataFrame, column:Series):
    iterador_index = 500000
    count = dataframe[column].count()
    print(count)
    cursor = connect().cursor()
    tempo_inicial_process = datetime.datetime.now()

    inserts = 0
    index_inicio = 1
    index_final = 500000
    registros = 0

    while inserts < count:

        insert = tupla(index_inicio,index_final,estabelecimentos)

        tempo_inicial = datetime.datetime.now()
        cursor.executemany("insert into business values(%s,%s,%s,%s,%s)", insert)
        connect().commit()
        connect().close()
        tempo_final = datetime.datetime.now()


        index_inicio += iterador_index
        index_final += iterador_index
        inserts += iterador_index
        registros += iterador_index
        print(f"Foram insertados mais {iterador_index} linhas, total de {registros} registros e levou "
              f"{timedelta(minutes=tempo_final.minute,seconds=tempo_final.second) - timedelta(minutes=tempo_inicial.minute,seconds=tempo_inicial.second)}")

    tempo_final_process = datetime.datetime.now()


    print(f'Para inserir todos os registros no banco, levou {timedelta(minutes=tempo_final_process.minute,seconds=tempo_final_process.second,hours=tempo_final_process.hour) - timedelta(minutes=tempo_inicial_process.minute,seconds=tempo_inicial_process.second,hours=tempo_inicial_process.hour)}')



print("Começando operação")
estabelecimentos = treatmentEstabelecimentos().select('cnpj_basico', 'nome_fantasia', 'bairro', 'uf', 'municipio')
estabelecimentos = estabelecimentos.toPandas()
iterador(estabelecimentos,'uf')





