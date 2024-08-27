# generate chdb FFM java code
chdb-generate:
  jextract --output src/main/java -t org.chdb.ffm --header-class-name LibChdb  -l chdb --include-struct local_result_v2 --include-function query_stable_v2 --include-function free_result_v2 src/main/headers/chdb.h

# query logs.csv
query:
  clickhouse local --query="select * from file('src/test/resources/logs.csv','CSV')" --format=JSON | jq