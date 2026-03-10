# ===============================================================
# PROJETO BIG DATA - ANÁLISE DE DADOS DE DELIVERY
# ===============================================================
# Objetivo:
# Realizar análises de dados utilizando Apache Spark e Spark SQL
# e gerar visualizações utilizando Matplotlib.
#
# Análises realizadas:
# 1 - Ranking de cidades por quantidade de pedidos
# 2 - Ranking de restaurantes por faturamento
# 3 - Comparação entre faturamento e tempo médio de entrega
# 4 - Top 3 restaurantes considerando múltiplos critérios
# ===============================================================


# ===============================================================
# 1. Importação das bibliotecas
# ===============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round
import matplotlib.pyplot as plt


# ===============================================================
# 2. Inicialização da SparkSession
# ===============================================================

spark = SparkSession.builder \
    .appName("BigData_FoodDelivery_Analytics") \
    .master("local[*]") \
    .getOrCreate()

# ===============================================================
# 2. Controle de exibição de erro
# ===============================================================
# Faz com que apareça apenas erros importantes que impactam na funcionalidade do codigo
spark.sparkContext.setLogLevel("ERROR")




# ===============================================================
# 3. Caminho do dataset
# ===============================================================

caminho_arquivo = "dados/delivery_dados_ficticios.csv"


# ===============================================================
# 4. Leitura do arquivo CSV
# ===============================================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(caminho_arquivo)


# ===============================================================
# 5. Criação da View SQL
# ===============================================================

df.createOrReplaceTempView("pedidos")


# ===============================================================
# ANÁLISE 1
# Ranking de cidades por quantidade de pedidos
# ===============================================================

print("\n====================================")
print("RANKING DE CIDADES POR PEDIDOS")
print("====================================")

ranking_cidades = spark.sql("""

SELECT
    cidade,
    total_pedidos,
    RANK() OVER (ORDER BY total_pedidos DESC) AS ranking_cidade

FROM (
        SELECT
            cidade,
            COUNT(*) AS total_pedidos
        FROM pedidos
        GROUP BY cidade
) t

""")

ranking_cidades.show()

# Convertendo para Pandas para gerar gráfico
df_pandas = ranking_cidades.toPandas()

plt.figure()
plt.bar(df_pandas["cidade"], df_pandas["total_pedidos"])

plt.title("Ranking de Cidades por Quantidade de Pedidos")
plt.xlabel("Cidade")
plt.ylabel("Total de Pedidos")

plt.xticks(rotation=45)

plt.show()


# ===============================================================
# ANÁLISE 2
# Ranking de restaurantes por faturamento
# ===============================================================

print("\n====================================")
print("RANKING DE RESTAURANTES POR FATURAMENTO")
print("====================================")

ranking_restaurantes = spark.sql("""

SELECT
    restaurante,
    faturamento_total,
    ROW_NUMBER() OVER (ORDER BY faturamento_total DESC) AS ranking_restaurante

FROM (
        SELECT
            restaurante,
            ROUND(SUM(valor_pedido),2) AS faturamento_total
        FROM pedidos
        GROUP BY restaurante
) t

""")

ranking_restaurantes.show()

df_pandas = ranking_restaurantes.toPandas()

plt.figure()
plt.bar(df_pandas["restaurante"], df_pandas["faturamento_total"])

plt.title("Ranking de Restaurantes por Faturamento")
plt.xlabel("Restaurante")
plt.ylabel("Faturamento Total")

plt.xticks(rotation=45)

plt.show()


# ===============================================================
# ANÁLISE 3
# Comparação entre faturamento e tempo médio de entrega
# ===============================================================

print("\n====================================")
print("COMPARAÇÃO ENTRE FATURAMENTO E TEMPO DE ENTREGA")
print("====================================")

comparacao_criterios = spark.sql("""

SELECT
    restaurante,
    faturamento_total,
    tempo_medio_entrega,
    DENSE_RANK() OVER (ORDER BY faturamento_total DESC) AS ranking_faturamento

FROM (
        SELECT
            restaurante,
            ROUND(SUM(valor_pedido),2) AS faturamento_total,
            ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega
        FROM pedidos
        GROUP BY restaurante
) t

""")

comparacao_criterios.show()

df_pandas = comparacao_criterios.toPandas()

plt.figure()
plt.scatter(
    df_pandas["faturamento_total"],
    df_pandas["tempo_medio_entrega"]
)

plt.title("Relação entre Faturamento e Tempo Médio de Entrega")
plt.xlabel("Faturamento Total")
plt.ylabel("Tempo Médio de Entrega")

plt.show()


# ===============================================================
# ANÁLISE 4
# Top 3 restaurantes considerando múltiplos critérios
# ===============================================================

print("\n====================================")
print("TOP 3 RESTAURANTES - ANÁLISE MULTICRITÉRIO")
print("====================================")

top3_restaurantes_sql = spark.sql("""

SELECT
    restaurante,
    cidade,
    faturamento_total,
    tempo_medio_entrega,

    RANK() OVER (ORDER BY faturamento_total DESC) AS rank_faturamento,
    RANK() OVER (ORDER BY tempo_medio_entrega ASC) AS rank_tempo

FROM (

    SELECT *
    FROM (

        SELECT
            restaurante,
            cidade,
            ROUND(SUM(valor_pedido),2) AS faturamento_total,
            ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega
        FROM pedidos
        GROUP BY restaurante, cidade

    ) base

    ORDER BY faturamento_total DESC
    LIMIT 3

) top3

""")

top3_restaurantes_sql.show()

df_pandas = top3_restaurantes_sql.toPandas()

plt.figure()
plt.bar(df_pandas["restaurante"], df_pandas["faturamento_total"])

plt.title("Top 3 Restaurantes por Faturamento")
plt.xlabel("Restaurante")
plt.ylabel("Faturamento Total")

plt.xticks(rotation=45)

plt.show()


# ===============================================================
# Encerramento da SparkSession
# ===============================================================

spark.stop()

print("\nProcessamento finalizado com sucesso.")



