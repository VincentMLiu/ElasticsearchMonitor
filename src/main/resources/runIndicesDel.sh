day=`date -d today '+Y%M%D'`
echo ${today}
java -cp ElasticsearchMonitor-1.0-SNAPSHOT.jar com.act.ElasticsearchMonitor.elasticsearch.IndicesDelete >> /home/elastic/EsMonitor/logs/indiceDel${today}.log