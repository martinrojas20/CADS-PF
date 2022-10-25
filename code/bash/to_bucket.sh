hdfs dfs -get ../common/data/jdefinitive ../data

echo 'Data local'

cd ../data/definitive

cat *.json > devinitive.json

gsutil cp definitive.json gs://reviews_reviews_cads/reviews

echo 'Data en bucket'
