# Proyecto ReBaF de CADS

Proyecto ReBaF de CADS

![Alt text](src/cads_logo_red.png?raw=true "")

## Doc

Contiene la documentación a lo largo del proyecto: planificación inicial, diagrama de gantt actualizado, consignas originales y presentaciones que se vayan realizando.

## Test.json

Archivo de reseñas de instrumentos musicales que se utilizó para hacer pruebas de script

## Src

Contiene imágenes que nos son de utilidad en el proyecto.

## EDA_ETL.md

Describe dichos procesos

## Scripts

La carpeta contiene distintos archivos de código que hemos utilizado:

### Pipeline

Se utiliza ejecuta el script pipeline.sh en la consola del clúster: 'bash pipeline.sh'.

En caso que se quiera hacer con una selección de archivos o con archivos nuevos (como el dataset de instrumentos), se deben hacer comentarios y cambios de nombre pertinentes, manteniendo la estructura. Demora 59 minutos y 40 segundos.

![Alt text](src/pipeline_time_cut.png?raw=true "")

Para obtener el tiempo, se agrega 'time ' al principio del comando de ejecución.

El resultado son los archivos en formato .json, que conservaremos a modo de Data Lake.

### ETL

Ejecutamos los códigos en etl_*.ipynb creamos nuevos archivos .json sobre los cuales trabajaremos, generando así nuestro Data Warehouse.

### Openers

Ejecutando los códigos en *_opener.ipynb obtenemos un pantallazo de nuestros datos originales.

## Gantt

![Alt text](src/gantt.png?raw=true "")

## DER

![Alt text](src/DER.jpeg?raw=true "")

## Boceto Dashboard

![Alt text](src/boceto_dash.jpeg?raw=true "")
