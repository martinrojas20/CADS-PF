# Henry-Team-PF

Proyecto ReBaF de CADS

![Alt text](src/CADS.jpg?raw=true "")

## Pipeline

Se utiliza ejecuta el script pipeline.sh en la consola del clúster: 'bash pipeline.sh'

En caso que se quiera hacer con una selección de archivos o con archivos nuevos (como el dataset de instrumentos), se deben hacer comentarios y cambios de nombre pertinentes, manteniendo la estructura. Demora X segs.

Para obtener el tiempo, se agrega 'time ' al principio del comando de ejecución

## EDA

Al ingresar a los datos nos hemos encontrado con nombres de columnas que no eran amenos, algunas columnas que no eran de utilidad, la columna fecha en un formato extraño y muchos falores faltantes.

## ETL

Ejecutamos los scripts etl_reviews.ipynb y etl_products.ipynb
