cd ../py

python3 etl.py

echo 'ETL realizada'

python3 text_proc.py

echo 'Procesamiento de texto realizado'

python3 join.py

echo 'Datas unidas'
