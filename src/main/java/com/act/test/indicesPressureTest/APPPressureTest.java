package com.act.test.indicesPressureTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class APPPressureTest {

    //获取系统核数
    static final int nThreads = Runtime.getRuntime().availableProcessors();

    public static CountDownLatch latch;

    public static void main(String[] args) {


        int n = nThreads; //默认线程为核数
        int bn = 2;
        int bs = 10;
        String number = nThreads + "";
        String batchNo = "2";
        String batchSize = "10";


        if(args.length == 2){
            batchNo = args[0]; //每个线程入多少批次
            batchSize = args[1];//每个线程每批入多少条
        }else if(args.length == 3){
            number = args[0]; ///总共多少个线程
            batchNo = args[1]; //每个线程入多少批次
            batchSize = args[2];//每个线程每批入多少条
        }else{
            System.err.println("参数个数必须为2或3");
            System.err.println("参数个数为2：batchNo  batchSize， 线程数为系统核数");
            System.err.println("参数个数为3：number batchNo  batchSize");
            System.exit(-1);
        }

        //总共多少个线程
        if(number!=null && !"".equals(number)){
            n = new Integer(number);
        }

        //每个线程入多少批次
        if(batchNo!=null && !"".equals(batchNo)){
            bn = new Integer(batchNo);
        }

        //每个线程每批入多少条
        if(batchSize!=null && !"".equals(batchSize)){
            bs = new Integer(batchSize);
        }

        latch = new CountDownLatch(n);


        Long start = System.currentTimeMillis();
        //开启多线程
        ExecutorService exs = Executors.newFixedThreadPool(n);
        try {
            //结果集
            List<TaskResultBean> list = new ArrayList<>();
            List<Future<TaskResultBean>> futureList = new ArrayList<>();

            //1.高速提交10个任务，每个任务返回一个Future入list
            for (int i = 0; i < n; i++) {
                futureList.add(exs.submit(new IndicesPressureAPPTask(latch,bn,bs)));
            }
            Long getResultStart = System.currentTimeMillis();
            System.out.println("结果归集开始时间=" + new Date());
            //2.结果归集，用迭代器遍历futureList,高速轮询（模拟实现了并发），任务完成就移除
            while(futureList.size()>0){
                Iterator<Future<TaskResultBean>> iterable = futureList.iterator();
                //遍历一遍
                while(iterable.hasNext()){
                    Future<TaskResultBean> future = iterable.next();
                    //如果任务完成取结果，否则判断下一个任务是否完成
                    if (future.isDone() && !future.isCancelled()){
                        //获取结果
                        TaskResultBean taskResultBean = future.get();
//                        System.out.println("任务i=" + taskResultBean.getThreadName() + "获取完成，移出任务队列！");
                        list.add(taskResultBean);
                        //任务完成移除任务
                        iterable.remove();

                    }else{
                        Thread.sleep(1);//避免CPU高速运转，这里休息1毫秒，CPU纳秒级别
                    }
                }
            }

            double totalGbSize = 0d;//总发送字段大小
            long totalSendCount = 0l;//总发送条数
            long totalSendTime = 0l;//总发送时间mills
            long esTotalDealTime = 0l; //ES总处理时间
            for(TaskResultBean taskResultBean : list){
                totalGbSize += taskResultBean.getGbSize();
                totalSendCount += taskResultBean.getSendCount();
                totalSendTime += taskResultBean.getSendTimeInMils();
                esTotalDealTime += taskResultBean.getEsDealingTimeInMils();
            }



            System.out.println("发送总大小(GB)：" + totalGbSize);
            System.out.println("发送总条数：" + totalSendCount);
            System.out.println("发送总时间(秒)：" + (totalSendTime/1000));
            System.out.println("ES总处理时间(秒)：" + (esTotalDealTime/1000));
            System.out.println("发送请求速度（条/秒）：" + totalSendCount/(totalSendTime/1000));
            System.out.println("发送请求速度（MB/秒）：" + (totalGbSize*1024)/(totalSendTime/1000));
            System.out.println("ES总处理时间（条/秒）：" + totalSendCount/(esTotalDealTime/1000));
            System.out.println("ES总处理时间（MB/秒）：" + (totalGbSize*1024)/(esTotalDealTime/1000));



            System.out.println("总耗时=" + (System.currentTimeMillis() - start) + ",取结果归集耗时=" + (System.currentTimeMillis() - getResultStart));
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            if(latch.getCount()==0){
                exs.shutdown();
                System.exit(0);
            }

        }
    }


}
