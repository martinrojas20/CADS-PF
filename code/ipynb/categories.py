from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('CADS').getOrCreate() #Abrimos el archivo
df = spark.read.json('../common/data/definitive')

def get_first(row):
    new = row.split(',',1)[0].strip()
    return new

df = df.fillna(value='Unifentified',subset='categories')

col_n = udf(get_first)
df = df.withColumn("categories", col_n(col("categories"))) 
df = df.drop('price')




df.write.json('../common/data/definitive2')


categorias = {
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

df = df.replace(categorias,'categories')