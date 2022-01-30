
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum,avg,max,min,mean,count
from pyspark.sql.functions import *
from time import sleep
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

print("imprimindo dados cadastrais")
sleep(2)
dadosCadastrais = [("3170", "16994-3", "57.646.732/0001-70", "Joao da Silva"), ("2144","17998-8","23.034.326/0001-35", "Marcio Souza Spetalii"), \
    ("3270","14489-0","57.646.732/0001-70", "Joao da Silva")]
dadosCadastraisColumns = ["agencia_","conta_", "cnpj", "nome"]
dadosCadastraisDF = spark.createDataFrame(data=dadosCadastrais, schema = dadosCadastraisColumns)

dadosCadastraisDF.printSchema()
dadosCadastraisDF.show(truncate=False)

print("imprimindo boletos")
sleep(2)
boleto = [("00190500954014481606906809350314337370000000100", "3170","16994-3","130,00","20/01/2002"),("10100500333014481606906809350314337370000000100","3270","14489-0","340,00","20/01/2002"), \
    ("18273645954014481606906809350314337370000000100","2144","17998-8","771,98","20/01/2002")]
boletoColumns = ["boleto","agencia","conta", "valor","data"]
boletoDF = spark.createDataFrame(data=boleto, schema = boletoColumns)
boletoDF.printSchema()
boletoDF.show(truncate=False)

print("juntando tudo -------------------------------------------------------------------------------------")
sleep(2)
dadosCadastraisDF.join(boletoDF,dadosCadastraisDF.agencia_ ==  boletoDF.agencia,"inner") \
     .show(truncate=False)


final = dadosCadastraisDF.join(boletoDF,dadosCadastraisDF.agencia_ ==  boletoDF.agencia,"inner") 
print("final")

df_basket_reordered = final.select("cnpj","agencia_","conta_","nome","valor","data","boleto").sort("cnpj")
df_basket_reordered.show()
