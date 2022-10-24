from pyspark.sql import SparkSession #Importamos lo necesario
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, FloatType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('CADS').getOrCreate() #Ingresamos a la sesión de Spark
products = spark.read.json("../common/data/products.json") #Abrimos el archivo Productos

products = products.drop('imUrl', '_corrupt_record', 'brand', 'description', 'salesRank') #Tumbamos las columnas que no sirven
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

reviews = spark.read.json("../common/data/reviews.json") #Abrimos el archivo Reviews

reviews = reviews.drop('helpful','summary','unixReviewTime','reviewerName') #Quitamos las columnas que no sirven
reviews = reviews.selectExpr('asin as productId', 'overall as rating', 'reviewerID as reviewerId', 'revieweText as revieweText', 'revieweTime as revieweTime') #Renombramos las columnas
reviews = reviews.fillna(value='',subset='reviewText') #Llenamos los textos nulos
reviews = reviews.withColumn("rating",reviews["rating"].cast("Integer")) #Convertimos rating a entero
reviews = reviews.withColumn("reviewTime",translate("reviewTime",",","")) #Corregimos el formato de la fecha
reviews = reviews.withColumn("reviewText",translate("reviewText",",","")) #Quitamos comas del texto
reviews = reviews.withColumn("reviewText",translate("reviewText",".","")) #Quitamos los puntos del texto
reviews = reviews.withColumn("reviewTime",translate("reviewTime"," ","-")) #Reformateamos la fecha
reviews = reviews.withColumn("reviewTime",to_date(col("reviewTime"),"MM-d-yyyy")) #Convertimos la fecha a fecha
reviews = reviews.na.drop() #Quitamos los registros con valores nulos

products = products.join(reviews,products.productId == reviews.productId,"leftsemi") #Quitamos de productos los id de reviews
reviews = reviews.join(products,reviews.productId == products.productId,"leftsemi") #Quitamos de reviews los id de productos

reviews.write.json('../common/data/reviews_clean') #Guardamos la data Comparada
products.write.json('../common/data/products_clean')