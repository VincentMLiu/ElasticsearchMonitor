package com.act.test;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SlowLogAnalyse {


    public static void main(String[] args) throws IOException {


        BufferedReader bf;
        bf = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:///Users/ThinkPad/Desktop/slowlogs.log"))));

        Pattern pattern = Pattern.compile("([1-9]\\d*\\.?\\d+)");



        Map<Double, String > sortMap = new HashMap<>();
        String line = "";
        int lineIndex = 1;
        while((line = bf.readLine())!=null){


            String[] time = line.split("\\s");
            Matcher matcher=pattern.matcher(time[4]);
            while(matcher.find()) {
                double timeMin = Double.parseDouble(Double.parseDouble(matcher.group(0)) + "" +lineIndex) ;
                sortMap.put(timeMin, line);
                lineIndex++;
            }



        }


        File outputFile = new File("C:///Users/ThinkPad/Desktop/slowlogs-result.log");

        OutputStream os = new FileOutputStream(outputFile);
        StringBuffer sb = new StringBuffer();

        Set set=sortMap.keySet();
        Object[] arr=set.toArray();
        Arrays.sort(arr);
        for(int i = arr.length-1 ; i >=0 ; i--){
            System.out.println(arr[i] + sortMap.get(arr[i]));
            sb.append(sortMap.get(arr[i]) + "\n");
        }

        os.write(sb.toString().getBytes());
        os.flush();
        os.close();


    }
}
