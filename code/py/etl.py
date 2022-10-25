from pyspark.sql import SparkSession #Importamos lo necesario
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import functions as F
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')

spark = SparkSession.builder.appName('CADS').getOrCreate() #Ingresamos a la sesión de Spark
products = spark.read.json("../common/data/products.json") #Abrimos el archivo Productos

products = products.drop('imUrl', '_corrupt_record', 'brand', 'description', 'salesRank','price') #Tumbamos las columnas que no sirven
products = products.selectExpr('asin as productId','categories as categories','related as related','title as title','price as price') #Renombramos las columnas
products = products.dropna(subset='productId') #Quitamos los productos no iDentificados
products = products.dropna(subset='title') #Quitamos los productos no iDentificados

#Cambiamos el tipo de dato de las columnas que nesecitamos.
products = products.withColumn('categories', F.col('categories').cast(StringType()))
products = products.withColumn('related', F.col('related').cast(StringType()))
products = products.withColumn('price', F.col('price').cast(FloatType()))

#Normalizamos los datos
products = products.withColumn("categories",translate("categories","[",""))
products = products.withColumn("categories",translate("categories","]",""))
products = products.withColumn("related",translate("related","[",""))
products = products.withColumn("related",translate("related","]",""))

def get_first(row): #Obtenemos la primera categoría
    new = row.split(',',1)[0].strip()
    return new

products = products.fillna(value='Unifentified',subset='categories') #Rellenamos donde no haya categoría

col_n = udf(get_first)
products = products.withColumn("categories", col_n(col("categories")))  #Aplicamos la función

categorias = { #Armamos un diccionario para normalizar las categorías
    'GPS & Navigation':'Electronics',
    'Camera & Photo':'Electronics',
    'All Electronics':'Electronics',
    'Computers':'Electronics',
    'MP3 Players & Accessories':'Electronics',
    'Buy a Kindle':'Electronics',
    'Furniture & D&#233;cor':'Home & Kitchen',
    'Kitchen & Dining':'Home & Kitchen',
    'Home Improvement':'Home & Kitchen',
    'Appliances':'Home & Kitchen',
    'All Beauty':'Beauty',
    'Luxury Beauty':'Beauty',
    'Amazon Fashion':'Beauty',
    'Office & School Supplies':'Office Products',
    'Car Electronics':'Automotive',
    'Baby Products':'Baby',
    'Collectibles & Fine Art':'Arts',
    'Musical Instruments':'Arts',
    'Unifentified':'Other',
    '':'Other',
    'Purchase Circles':'Other',
    'Gift Cards':'Other',
    'Software':'Digital Content',
    'Digital Music':'Digital Content',
    'Kindle Store':'Digital Content',
    'Video Games':'Digital Content'
    }

products = products.replace(categorias,'categories') #Normalizamos las categorías

reviews = spark.read.json("../common/data/reviews.json") #Abrimos el archivo Reviews

reviews = reviews.drop('helpful','summary','unixReviewTime','reviewerName') #Quitamos las columnas que no sirven
reviews = reviews.selectExpr('asin as productId', 'overall as rating', 'reviewerID as reviewerId', 'reviewText as reviewText', 'reviewTime as reviewTime') #Renombramos las columnas
reviews = reviews.fillna(value='',subset='reviewText') #Llenamos los textos nulos
reviews = reviews.withColumn("rating",reviews["rating"].cast("Integer")) #Convertimos rating a entero
reviews = reviews.withColumn("reviewTime",translate("reviewTime",",","")) #Corregimos el formato de la fecha
reviews = reviews.withColumn("reviewText",translate("reviewText",",","")) #Quitamos comas del texto
reviews = reviews.withColumn("reviewText",translate("reviewText",".","")) #Quitamos los puntos del texto
reviews = reviews.withColumn("reviewTime",translate("reviewTime"," ","-")) #Reformateamos la fecha
reviews = reviews.withColumn("reviewTime",to_date(col("reviewTime"),"MM-d-yyyy")) #Convertimos la fecha a fecha
reviews = reviews.na.drop() #Quitamos los registros con valores nulos

products = products.join(reviews,products['productId'] == reviews['productId'],"leftsemi") #Quitamos de productos los id de reviews
reviews = reviews.join(products,reviews['productId'] == products['productId'],"leftsemi") #Quitamos de reviews los id de productos

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

products = products.select(F.col("productId"),F.col("rating"),F.col("reviewerId"),F.col("reviewText"),F.col("reviewTime"),F.split(F.col("reviewText")," ").alias("tokens")) #Tokenizamos

sid = SentimentIntensityAnalyzer() #Análisis de sentimiento

products = products.withColumn('rating', F.col('rating').cast('Integer')) #Casteamos rating como entero

col_n = F.udf(lambda row: float(sid.polarity_scores(row)['compound'])) #Análisis de sentimiento
products = products.withColumn("sentiment_float", col_n(F.col("reviewText")).cast('Float'))
col_n = F.udf(condition)
products = products.withColumn("sentiment", col_n(F.col("sentiment_float")).cast('Integer')) #Discretizamos el sentimiento

col_n = F.udf(uso_proc)
products = products.withColumn("facilidadUso", col_n(F.col("tokens")).cast('Integer')) #Creamos facilidadUso
col_n = F.udf(calidad_proc)
products = products.withColumn("calidad", col_n(F.col("tokens")).cast('Integer')) #Creamos calidad
col_n = F.udf(precio_proc)
products = products.withColumn("precio", col_n(F.col("tokens")).cast('Integer')) #Creamos precio

col_n = F.udf(positivity) #Creamos un índice de positividad
products = products.withColumn("posIndex", col_n(F.col("rating")).cast('Integer'))

col_n = F.udf(multiplier)
products = products.withColumn("calidad", col_n(F.col("calidad"),F.col('posIndex')).cast('Integer')) #Ajustamos el signo de las variables
products = products.withColumn("facilidadUso", col_n(F.col("facilidadUso"),F.col('posIndex')).cast('Integer'))
products = products.withColumn("precio", col_n(F.col("precio"),F.col('posIndex')).cast('Integer'))

products = products.drop('posIndex','tokens','sentiment_float','reviewText') #Eliminamos las tablas que ya no necesitamos

joined = reviews.join(products,'productId',"full") #Unimos las trablas según productId

joined.write.json('../common/data/definitive') #Guardamos el df