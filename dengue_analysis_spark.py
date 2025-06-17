from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Iniciar sessão Spark
spark = SparkSession.builder \
    .appName("Analise de Dengue com PySpark") \
    .getOrCreate()

# Ler arquivo CSV
df = spark.read.csv("dengue.csv", header=True, inferSchema=True)
df.show()

# Casos por ano
casos_ano = df.groupBy("ano").sum("casos").withColumnRenamed("sum(casos)", "total_casos")
casos_ano.show()

# Casos graves
df.filter(col("classificacao") == "Grave").show()

# Converter para Pandas
casos_pd = casos_ano.toPandas()

# Gráfico
plt.bar(casos_pd["ano"], casos_pd["total_casos"], color="green")
plt.title("Casos de Dengue por Ano")
plt.xlabel("Ano")
plt.ylabel("Total de Casos")
plt.tight_layout()
plt.savefig("grafico_dengue_ano.png")
plt.show()

spark.stop()
