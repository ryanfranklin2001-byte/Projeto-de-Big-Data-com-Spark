from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round

#===================================================
# 1- Criando SparkSession
#===================================================
spark = SparkSession.builder \
    .appName("BigData_FoodDelivery") \
    .master("local[*]") \
    .getOrCreate()

#===================================================
# 2- Caminho do arquivo
#===================================================

caminho_arquivo = "dados/delivery_dados_ficticios.csv"

#===================================================
# 3- Leitura do CSV (atenção ao separador ;)
#===================================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(caminho_arquivo)

#===================================================
# Criando a view
#===================================================

df.createOrReplaceTempView("pedidos")

#===================================================
# 1. Ranking de cidades por quantidade de pedidos
#===================================================

print("Ranking de cidades por quantidade de pedidos")

ranking_cidades = spark.sql("""
SELECT
    RANK() OVER (ORDER BY total_pedidos DESC) AS ranking_cidade,
    cidade,
    total_pedidos
FROM (
    SELECT
        cidade,
        COUNT(*) AS total_pedidos
    FROM pedidos
    GROUP BY cidade
) t
""")

ranking_cidades.show()

#===================================================
# 2 - Ranking dos Restaurantes
#===================================================

print("Ranking de restaurantes por faturamento")

ranking_restaurantes = spark.sql("""
SELECT
    RANK() OVER (ORDER BY faturamento DESC) AS ranking_restaurante,
    restaurante,
    faturamento
FROM (
    SELECT
        restaurante,
        ROUND(SUM(valor_pedido),2) AS faturamento
    FROM pedidos
    GROUP BY restaurante
) t
""")

ranking_restaurantes.show()

#===================================================
# 3 - Faturamento x Tempo Medio
#===================================================

print("Comparação entre faturamento e tempo médio de entrega")

comparacao_criterios = spark.sql("""
SELECT
    restaurante,
    ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega,
    ROUND(SUM(valor_pedido),2) AS faturamento_total
FROM pedidos
GROUP BY restaurante
ORDER BY faturamento_total DESC
""")

comparacao_criterios.show()
