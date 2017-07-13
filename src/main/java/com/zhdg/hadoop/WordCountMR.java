package com.zhdg.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Lucario
 *
 */
public class WordCountMR extends Configured implements Tool {
	/**
	 * 
	 * LongWritable,Text:每行内容偏移量，每行的内容String
	 * Text,IntWritable:每一行每个单词，数量
	 * Context 上下文 将map输出的key value 分发给reduce的输入
	 *   /input/wordcount.txt
	 *   一行一行都进来Mapper按一行一行的处理
	 *   key longWritable的每一行的偏移量
	 *   value Text类型每一行的内容
	 * @author lx
	 *
	 */

	//Map
	//继承map方法(LongWritable key,Text value,Context context)方法
	public static class WordCountMapper extends Mapper<LongWritable,
	Text,Text,IntWritable>{
		private Text mapOutputKey= new Text();
		private IntWritable mapOutputValue = new IntWritable(1);
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			System.out.println("-----------------开始执行map-------------");
			//将Text类型的内容转化成String类型
			String line= value.toString();
			//将String类型的每一行内容按照空格切分为String数组,然后获取分割后的单词给“1”值
			String[] strs = line.split(" ");
			for(String a :strs){
				//获取分割后的单词
				mapOutputKey.set(a);
			    //在文本上写入 单词：“1”
				context.write(mapOutputKey, mapOutputValue);
			}
		}
		
	}
	
	/**
	 * shuffle  过程 
	 * partition (分区):将map输出交给具体的reduce去处理
	 * group (分组):将map输出结果中相同的key分到一组
	 *
	 * @author lx
	 *
	 */
	//Reduce
	//继承reduce方法(Text key,Iterable<InWritable> value,Context context)
	public static class wordCountReducer extends Reducer<Text,
	IntWritable,Text,IntWritable>{
		/**
		 * Text key(Map输出的一个一个的单词)
		 * Iterable<IntWritable> values (相同的单词，出现的次数放到一个集合)
		 */
		@Override
		protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			System.out.println("-----------------开始执行reduce-------------");
			//定义临时变量 用来接收相同单词出现个数的综合
			int sum = 0;	
			for(IntWritable value: values){
				//累加相同单词的“1”
				sum += value.get();
			}
			//在文本上写上单词：累加的“1”
			context.write(key, new IntWritable(sum));
		}
	}
	/**
	 * Driver方法是程序执行的入口
	 * 相当于Yarn的一个客户端
	 * 参考bin/yarn jar ......example.jar wordcount /input/wc.txt /out
	 * 封装mr的程序运行的参数，并指定jar包
	 * 提交 yarn,并等待执行的结果和进度反馈
	 * @param args
	 */
	public int run(String[] args) throws Exception {
		//1.获取hadoop配置信息
		System.out.println("-----------------开始执run-------------");
		Configuration conf = new Configuration();
		//core-default.xml, core-site.xml
		
		//2.生成job
		Job job=Job.getInstance(conf,
				this.getClass().getName()
				);
		//打成jar包并指定jar包所在路径
		
		
		System.out.println("job:"+this.getConf());
		//core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml

		//把类生成jar包
		job.setJarByClass(getClass());
		//3.设置job的内容
		// inputDir ->map -> reduce-> outPutdir
		//3.1指定输入路径
		Path inPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inPath);
		
		//3.2 map阶段
		job.setMapperClass(WordCountMapper.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(IntWritable.class);
		
		//3.3 reduce阶段
		job.setReducerClass(wordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//3.4指定输出路径
		Path outPath =new Path(args[1]);
		FileSystem fs =outPath.getFileSystem(conf);
		//判断目标文件夹是否存在，如果存在就删除
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		//4.提交job运行并得到结果,也可以使用job.submit(),但只有结果，没有进度反馈
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		//System.out.println("-----------------开始执行main-------------");
		Configuration conf = new Configuration();
		
/*		args = new String[]{
			"hdfs://lucario.m:8020/input/abcd.txt",
			"hdfs://lucario.m:8020/output4"
		};*/
		
		int status =ToolRunner.run(conf,
				                   new WordCountMR(),
				                   args);
		
		System.exit(status);

	}
}
