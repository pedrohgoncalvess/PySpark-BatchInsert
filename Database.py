import datetime
from datetime import timedelta
from InitSpark import initDataframe, initSparkSession as session
from Treatment import treatmentEstabelecimentos
import psycopg2
from psycopg2 import connect
from psycopg2.extras import execute_values
from typing import Iterable,List,Tuple
from types import FunctionType
from typing import Tuple


def tupla():
    lenght_tuple = 10000000
    df = treatmentEstabelecimentos().select('cnpj_basico','nome_fantasia','bairro','uf','municipio').limit(lenght_tuple)


    l = []
    for i in df.collect():
        l.append(tuple(i))

    return l


def connect():

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user='postgres',
        password='fodao002')
    conn.autocommit = True
    return conn

def temp_execucao(func:FunctionType) -> FunctionType:
    def execute():
        tempo_inicial = datetime.datetime.now()
        func()
        connect().commit()
        connect().close()
        tempo_final = datetime.datetime.now()
        print(f"Começou as {str(tempo_inicial)} e terminou as {str(tempo_final)}")
        print(f'Tempo levado {timedelta(seconds=tempo_final.second,minutes=tempo_final.minute) - timedelta(seconds=tempo_inicial.second,minutes=tempo_inicial.minute) }' )
        print(f'Linhas inseridas {lenght_tuple}')

    return execute


@temp_execucao
def insert_db(l:Tuple):
    return connect().cursor().executemany("insert into business values(%s,%s,%s,%s,%s)",l)


