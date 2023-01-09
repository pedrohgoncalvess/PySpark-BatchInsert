from Treatment import treatmentEstabelecimentos
from pyspark.sql import functions as f
from types import FunctionType


class Views:

    def init_df(self):
        estabelecimentos = treatmentEstabelecimentos()
        return estabelecimentos



    def groupbyYear(self):
         return self.init_df().select('cnpj_basico','cnpj_ordem','situacao_especial',f.year('data_situacao_cadastral').alias('ano_situ_cadast')).show(5,False)
                    #select padrao do SQL selecionando as colunas         #f.year é função para trazer apenas o ano da coluna dando um alias pra coluna


    def nullValues(self):
        return self.init_df().select([f.count(f.when(f.isnull(c),1)).alias(c) for c in self.init_df().columns]).show() #CONTA QUANTOS VALORES NULOS TEM NAS COLUNAS


    def orderBy(self):
        return self.init_df().select('cnpj_basico','cnpj_ordem','situacao_especial'
                            ,f.year('data_situacao_cadastral').alias('ano_situ_cadast')).orderBy('ano_situ_cadast',ascending=False)\
                            .show(5,False) #ORDER BY


    def likeQuery(self):
        return self.init_df().where(self.init_df().nome_fantasia.like('%PEDRO%')).show(truncate=False) #LIKE IGUAL AO SQL



    def orderByConditions(self):
        return self.init_df().select('cnpj_basico','cnpj_ordem','situacao_especial'
                            ,f.year('data_situacao_cadastral').alias('ano_situ_cadast')).orderBy(['ano_situ_cadast','situacao_especial'],ascending=[False,False])\
                            .show(5,False) #ORDER BY COM MAIS DE UM CONDICAO

    # estabelecimentos.where("capital_social_empresa=50").show(5,False) #SELECT COM WHERE SEM SELECT ELE TRAS TODAS AS COLUNAS

    # estabelecimentos.select("nome_fantasia").where(estabelecimentos.nome_fantasia.startswith("PEDRO")).show(5,False) #STARTA COM X STRIN || REGEX


def main():
    main = Views()
    print(main.orderBy())

if __name__ == '__main__':
    main()