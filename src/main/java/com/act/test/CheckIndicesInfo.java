package com.act.test;

import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.apache.lucene.queryparser.surround.query.SrndTruncQuery;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckIndicesInfo {
    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient;

    private static int dataNodeNum = 0;


    private static Date today = new Date();

    public static void main(String[] args) throws IOException {


//        String esHost = "localhost";
//        int esPort = 19203;
//        restClient = RestClient.builder(
//                new HttpHost(esHost, esPort, "http"))
//                .setMaxRetryTimeoutMillis(1000000)
//                .build();

        //获取所有索引信息

        FileReader fr = new FileReader(new File("D:\\Desktop\\indices.txt"));
        Map<Pattern, ArrayList<String>> patternMap = new HashMap<Pattern,ArrayList<String>>();
        String templateList = "app_mixed\\w+,app_store\\w+,app_yun\\w+,fw_log_atd\\w+,obj_event_secure\\w+,rpt_event_secure\\w+,gynetres\\w+,industry_atd\\w+,industry_dpi\\w+,industry_gynetres\\w+,industry_jmr_ipunit\\w+,jmr\\w+,malware\\w+,datasmart\\w+,judgeresult\\w+,dns\\w+_filteresmodel,dns\\w+_basedatamonitoresmodel,dns\\w+_monitoresmodel,dns\\w+_activeip,dns\\w+_activedomain,dns\\w+_visitresultesmodel,dns\\w+_provincetld,dns\\w+_ipdomainrelation,dns_\\w+_ipdomainrelation_copy,dns_\\w+_ipdomainrelation_copy,cdnactivedomainnoise\\w+,cdnfilterdomainnoise\\w+,cdnmonitordomainnoise\\w+,dnsauthparsenoise\\w+,dnsauthtrusteeshipnoise\\w+,dnsparsecountnoise\\w+,dnsrecursionparsenoise\\w+,dnsregnoise\\w+,dnsspecifiedfilternoise\\w+,idcactivedomainnoise\\w+,idcbasicdomainnoise\\w+,idcfilterdomainnoise\\w+,idcmonitordomainnoise\\w+,ircsactivedomainnoise\\w+,ircsbasicdomainnoise\\w+,ircsfilterdomainnoise\\w+,ircsmonitordomainnoise\\w+,blockdomainip\\w+,domainchange\\w+,domainipesbase\\w+,firstdomain\\w+,firstdomainip\\w+,housestatistics\\w+,ip_visitcount\\w+,ipstatistics\\w+,t_dwd_cdn_house_ip_conflictinfo\\w+,t_dwd_idc_house_ip_conflictinfo\\w+,t_dwd_ircs_ip_conflictinfo\\w+,t_ip_control\\w+,tdmdomainipbase\\w+,tdmdomainipbaseconflictinfo\\w+,tdmdomainipbaseconflictinfo_copy\\w+,tldstatistics\\w+,topdomainchange\\w+,topdomaindnsvisitcounts\\w+,topdomainipjoincounts\\w+,topdomainjoincount\\w+";
        String[] templateListSpli = templateList.split(",");
        for(String templateStr : templateListSpli){
            Pattern columnP = Pattern.compile(templateStr);
            patternMap.put(columnP,new ArrayList<String>());
        }

        BufferedReader br = new BufferedReader(fr);

        String line = "";

        List<String> extIndices = new ArrayList<String>();
        while((line = br.readLine()) !=null){

            boolean match = false;
            for(Pattern pt: patternMap.keySet()){
                Matcher mt =  pt.matcher(line);
                if(mt.matches()){
                    ArrayList<String> indexList = patternMap.get(pt);
                    indexList.add(line);
                    patternMap.put(pt,indexList);
                    match = true;
                    break;
                }
            }

            if(!match){
                extIndices.add(line);
            }

        }


        for(Map.Entry<Pattern, ArrayList<String>> entry: patternMap.entrySet()){
            System.out.println(entry.getKey().pattern() + ":" + entry.getValue());
        }
        System.out.println(extIndices);


    }









}
