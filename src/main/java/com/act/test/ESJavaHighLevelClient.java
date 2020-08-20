package com.act.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ESJavaHighLevelClient {



    public static void main(String[] args){
        Pattern indexPettern = Pattern.compile("industry_\\w+");
        Matcher matcher = indexPettern.matcher("industry_atd_20200909");
        System.out.println(matcher.matches());
        System.out.println(matcher.find());

    }



}
