package com.act.test.indicesPressureTest;

public class TaskResultBean {




    //线程名称
    private String threadName;

    //每个线程总共的发送gb大小
    private double gbSize;

    //ES返回的处理总时间
    private long esDealingTimeInMils;

    //压测程序发送时间
    private long sendTimeInMils;

    //线程每分钟发送的条数
    private long sendCountPerMin;

    //线程每分钟发送的条数
    private long sendCount;

    //线程每分钟发送的数据大小MB
    private long sendSizePerMin;

    public TaskResultBean(String threadName){
        this.threadName = threadName;
    }


    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public long getEsDealingTimeInMils() {
        return esDealingTimeInMils;
    }

    public void setEsDealingTimeInMils(long esDealingTimeInMils) {
        this.esDealingTimeInMils = esDealingTimeInMils;
    }

    public long getSendTimeInMils() {
        return sendTimeInMils;
    }

    public void setSendTimeInMils(long sendTimeInMils) {
        this.sendTimeInMils = sendTimeInMils;
    }

    public long getSendCountPerMin() {
        return sendCountPerMin;
    }

    public void setSendCountPerMin(long sendCountPerMin) {
        this.sendCountPerMin = sendCountPerMin;
    }

    public long getSendSizePerMin() {
        return sendSizePerMin;
    }

    public void setSendSizePerMin(long sendSizePerMin) {
        this.sendSizePerMin = sendSizePerMin;
    }

    public double getGbSize() {
        return gbSize;
    }

    public void setGbSize(double gbSize) {
        this.gbSize = gbSize;
    }


    public long getSendCount() {
        return sendCount;
    }

    public void setSendCount(long sendCount) {
        this.sendCount = sendCount;
    }
}
