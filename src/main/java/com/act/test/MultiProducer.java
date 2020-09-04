package com.act.test;


public class MultiProducer {


	//获取系统核数
	static final int nThreads = Runtime.getRuntime().availableProcessors();


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

		
		for(int i =0 ; i < n ; i++){
			IndicesPressureProducer k = new IndicesPressureProducer(n,bn,bs);
			k.start();
		}
		
	}



}
