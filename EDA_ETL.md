# EDA-ETL

Explicaremos aquí cómo realizamos dichos procesos

Ambos sets de datos ya estaban parcialmente procesados desde la fuente.

La selección abarca datos entre mayo de 1996 hasta julio de 2014.

Se obtuvieron de la fuente asignada: <<http://jmcauley.ucsd.edu/data/amazon/links.html>>

Se utilizan las reviews de productos con al menos 5 reseñas.

Los archivos originales se conservan.

## Productos

Nos encontramos con un total de 9 millones de productos, aunque con muchos datos nulos.

### Estructurados en las siguientes columnas

asin - ID del producto. String. Eliminamos los registros que tenían este dato nulo. Renombrada productId

title - Nombre del producto. String. Todo en orden.

price - Precio en dólares. Float. La mitad de los datos faltan.

imUrl - URL de la imagen del producto. No aporta información, así que la descartaremos.

related - Productos relacionados (comprados juntos, también revisados, comprados por el mismo usuario, comprados luego de verlos). Estructurados en una lista de listas. Lo convertimos a String y lo limpiamos para que quede como una seguidilla de Ids de productos.

salesRank - Información sobre cómo rankea en ventas. Faltaba la mayor parte de los datos y los restantes eran números gigantes. Faltaba información y no nos resultaba de utilidad, por lo que la descartamos.

brand - Nombre de la marca del producto. Sólo estaba el 10% de los datos, por lo que la descartamos. Sería información útil.

categories - Lista de las categorías a las que pertenece el producto. Como related la convertimos en String y la limpiamos un poco.

### Resultante Productos

![Alt text](src/etl_products.png?raw=true "")

## Reviews

Hacen un total de 40 millones de reseñas, sin datos nulos de importancia.

### Estructuradas en las siguientes columnas

reviewerID - ID del usuario. String.

asin - ID del producto. String. Renombrado productId.

reviewerName - Nombre del usuario. Teniendo ya el Id de los usuarios decidimos descartarla.

helpful - Puntaje de utilidad de la reseña (votos externos), e.g. 2/3. No aportaba información de utilidad por lo que la descartamos.

reviewText - Texto de la reseña. String.

overall - Puntaje dado al producto. Integer. Renombrado rating.

summary - Resumen de la reseña. Al tener ya el texto de la reseña, decidimos que un resumen no era de utilidad, por lo que la la descartamos.

unixReviewTime - Fecha de la reseña (unix time). Teniendo ya la fecha en un formato más útil decidimos descartarla.

reviewTime - Fecha de la reseña. La convertimos a Date.

### Resultante Reviews

![Alt text](src/etl_reviews.png?raw=true "")
