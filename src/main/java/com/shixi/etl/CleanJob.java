package com.shixi.etl;

import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CleanJob extends Configured implements Tool{

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try{
			int res = ToolRunner.run(conf, new CleanJob(), args);
			System.exit(res);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	 class Parser{
		/**
		 * 解析时间字符串
		 */
		public final SimpleDateFormat INDATE = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss",Locale.ENGLISH);
		public final SimpleDateFormat OUTDATE = new SimpleDateFormat("yyyyMMddHHmmss");
	}
	class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
	}
}
