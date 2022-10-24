from pyspark.sql import SparkSession #Importamos lo necesario
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('CADS').getOrCreate() #Abrimos los archivos
products = spark.read.json("../common/data/products_clean")
reviews = spark.read.json("../common/data/reviews_processed")

joined = reviews.join(products,'productId',"full") #Unimos las trablas según productId

joined.write.json('../common/data/definitive') #Guardamos el df