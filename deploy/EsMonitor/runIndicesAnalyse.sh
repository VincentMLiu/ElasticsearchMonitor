today=`date -d today '+%Y%m%d'`
echo ${today}
java -cp ElasticsearchMonitor-1.0-SNAPSHOT.jar com.act.ElasticsearchMonitor.elasticsearch.IndicesAnalyse >> ./logs/IndicesAnalyse${today}.log