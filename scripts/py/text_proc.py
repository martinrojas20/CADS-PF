from nltk.sentiment.vader import SentimentIntensityAnalyzer  #Importamos lo necesario
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

spark = SparkSession.builder.appName('sent').getOrCreate() #Abrimos el archivo
df = spark.read.json('../common/data/reviews_etl')

#Definimos los diccionarios
calidad = ['quality','condition','make','made','plastic','plastics','material','materials','finished','well-finished','solid','sturdy','durable','well-made','broken','weak','breaks','fragile']
uso= ['use','using','easy','apply','employ','manipulate','effortless','straightforward','uncomplicated','difficile','hard', 'install', 'simple', 'complicated', 'difficult', 'difficulty', 'quickly', 'understand','learn']
precio = ['price','cost','costs','pays','costly','overpriced','economical','low-cost','low-priced' 'expensive', 'cheap', 'cheaper', 'cheapest', 'worth']

def condition(row):    #Función de Discretizado
    if row > 0.6: return 5
    elif row > 0.2 : return 4
    elif row < -0.6 : return 1
    elif row < -0.2 : return 2
    else : return 3

def limiter(num):       #Función límite de cuenta
    if num > 0: return 1
    elif num < 0: return -1
    else: return 0

def uso_proc(row): #Función uso
    num = 0
    for i in row:
        if i.lower() in uso: num += 1
    return int(limiter(num))

def calidad_proc(row): #Función calidad
    num = 0
    for i in row:
        if i.lower() in calidad: num += 1
    return limiter(num)

def precio_proc(row): #Función precio
    num = 0
    for i in row:
        if i.lower() in precio: num += 1
    return limiter(num)

def positivity(val1):
    return limiter(val1 -3)

def multiplier(val1,val2):
    return val1*val2

df = df.select(F.col("productId"),F.col("rating"),F.col("reviewerId"),F.col("reviewText"),F.col("reviewTime"),F.split(F.col("reviewText")," ").alias("tokens")) #Tokenizamos

sid = SentimentIntensityAnalyzer() #Análisis de sentimiento

df = df.withColumn('rating', F.col('rating').cast('Integer')) #Casteamos rating como entero

col_n = F.udf(lambda row: float(sid.polarity_scores(row)['compound'])) #Análisis de sentimiento
df = df.withColumn("sentiment_float", col_n(F.col("reviewText")).cast('Float'))
col_n = F.udf(condition)
df = df.withColumn("sentiment", col_n(F.col("sentiment_float")).cast('Integer')) #Discretizamos el sentimiento

col_n = F.udf(uso_proc)
df = df.withColumn("facilidadUso", col_n(F.col("tokens")).cast('Integer')) #Creamos facilidadUso
col_n = F.udf(calidad_proc)
df = df.withColumn("calidad", col_n(F.col("tokens")).cast('Integer')) #Creamos calidad
col_n = F.udf(precio_proc)
df = df.withColumn("precio", col_n(F.col("tokens")).cast('Integer')) #Creamos precio

col_n = F.udf(positivity) #Creamos un índice de positividad
df = df.withColumn("posIndex", col_n(F.col("rating")).cast('Integer'))

col_n = F.udf(multiplier)
df = df.withColumn("calidad", col_n(F.col("calidad"),F.col('posIndex')).cast('Integer')) #Ajustamos el signo de las variables
df = df.withColumn("facilidadUso", col_n(F.col("facilidadUso"),F.col('posIndex')).cast('Integer'))
df = df.withColumn("precio", col_n(F.col("precio"),F.col('posIndex')).cast('Integer'))

df = df.drop('posIndex','tokens','sentiment_float','reviewText') #Eliminamos las tablas que ya no necesitamos

df.write.json('../common/data/reviews_processed.json') #Guardamos el json