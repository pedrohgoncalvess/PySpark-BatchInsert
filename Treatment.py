from InitSpark import initDataframe, initSparkSession as session

from pyspark.sql import functions as f #IMPORTANDO FUNÇOES DO SPARK
from pyspark.sql.types import DateType,StringType,DoubleType  #IMPORTANDO TIPOS DE COLUNAS
import pyspark



def treatmentEstabelecimentos() -> pyspark.sql.dataframe.DataFrame:

    estabelecimentos = initDataframe(session())

    estabsColNames = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
                      'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                      'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade', 'cnae_fiscal_principal',
                      'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro', 'numero', 'complemento',
                      'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                      'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_da_situacao_especial']  #NOME DAS NOVAS COLUNAS

    for index, col in enumerate(estabsColNames):
        estabelecimentos = estabelecimentos.withColumnRenamed(f'_c{index}', col) #RENAME DAS COLUNAS COM LAÇO


    estabelecimentos = replaceFill(estabelecimentos)


    estabelecimentos = estabelecimentos.withColumn(
        "data_situacao_cadastral",f.to_date(estabelecimentos.data_situacao_cadastral.cast(StringType()),'yyyyMMdd')).\
        withColumn(

        "data_de_inicio_atividade",f.to_date(estabelecimentos.data_de_inicio_atividade.cast(StringType()),'yyyyMMdd')).\
        withColumn(

        "data_da_situacao_especial",f.to_date(estabelecimentos.data_da_situacao_especial.cast(StringType()),'yyyyMMdd') #CONVERTENDO O TYPE DAS COLUNAS EM DATE
    )                               #F É PARA FUNCTIONS E O TO DATE É A FUNCAO, ESTABELECIMENTOS.NOME_COLUMN OU ESTABELECIMENTOS['NOME_COLUM'] TBM FUNCIONARIA


    return estabelecimentos



def replaceFill(estabelecimentos:pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    estabelecimentos.na.fill(0)
    estabelecimentos.na.fill('n')

    return estabelecimentos




