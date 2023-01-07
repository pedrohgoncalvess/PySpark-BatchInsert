

def initSparkSession():
    import os
    import zipfile
    import pandas as pd

    os.environ['SPARK_HOME'] = r"C:\Users\Pedro\Desktop\WorkSpace\Spark\spark-3.2.2-bin-hadoop2.7"
    os.environ['HADOOP_HOME'] = r'C:\Users\Pedro\Desktop\WorkSpace\Spark\spark-3.2.2-bin-hadoop2.7\hadoop'

    import findspark

    findspark.init()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master('local[*]').getOrCreate()
    #spark = Sparksession.newSession()


    return spark


def initDataframe(spark):
    path = r'C:\Users\Pedro\Desktop\WorkSpace\Spark\estabelecimentos'
    estabelecimentos = spark.read.csv(path,sep=';',inferSchema=True)

    return estabelecimentos

