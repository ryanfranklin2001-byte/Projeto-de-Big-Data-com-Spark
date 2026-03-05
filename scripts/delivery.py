from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, round, sum

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Projeto Integrador - Desenvolvimento de sistemas") \
    .getOrCreate()

#===============================
#ler arquivo CSV
#===============================
caminho_dados="dados/delivery_dados_ficticios.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(caminho_dados)

print("Dados carregados com sucesso")

print("\n Estrutura do data frame:")
df.printSchema()

print("\nPrimeiro Registros:")
df.show(5)

total_registros = df.count()
print(f"\nTotal de registros: {total_registros}")

#===================================================
# 5. Análise simples (Big Data na prática)
#===================================================

#===================================================
# Total de vendas por restaurante
#===================================================

print("\nTotal de vendas por restaurante:")
df.groupBy("restaurante") \
  .agg(count("*").alias("QTD_Vendas")) \
  .show()


#===================================================
# Valor médio das vendas
#===================================================

print("\nValor médio das vendas:")
df.select(round(avg("valor_pedido")).alias("media_vendas")).show()


#===================================================
#  Total de pedidos por cidade
#===================================================
pedidos_cidade = df.groupBy("cidade").agg(
    count("*").alias("total_pedidos"),
    round(sum("valor_pedido"),2).alias("valor_total"),
    round(avg("tempo_entrega_min"),2).alias("tempo_medio_entrega")
)

pedidos_cidade.show()

#===================================================
# Desempenho por restaurante
#===================================================
restaurantes = df.groupBy("restaurante").agg(
    count("*").alias("qtd_pedidos"),
    round(avg("tempo_entrega_min"),2).alias("tempo_medio")
)

restaurantes.show()

#===================================================    
# Finalizando o spark
#===================================================

spark.stop()




