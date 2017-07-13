package ns;

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

public  class SumFlowMapReduce extends Configured implements Tool {
	
	
	public static class SumFlowMapper extends Mapper<LongWritable,Text,PairWritable,IntWritable>{
		private PairWritable mapOutputKey = new PairWritable();
		private IntWritable mapOutputValue = new IntWritable();
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			//a	100 
			String line = value.toString();
			
			String[] strs = line.split("\t");
			String name = strs[0];
			int money = Integer.valueOf(strs[1]);
		//	for(String str:strs){
				mapOutputKey.set(name,money);
				mapOutputValue.set(money);
				context.write(mapOutputKey, mapOutputValue);
			//}
		}
	}
	public static class SumFlowReducer extends Reducer<PairWritable,IntWritable,Text,IntWritable>{
		private Text outPutKey = new Text();
		
		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			 //   outPutKey = new Text(key.getName());
	        // context.write(outPutKey, new IntWritable(key.getMoney()));
	          for(IntWritable money:values){
	        	   outPutKey.set(key.getName());
	        	   context.write(outPutKey, money);
	           }
		}
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, this.getClass().getName());
		 job.setJarByClass(this.getClass());
		 
		 Path inPath = new Path(args[0]);
		 FileInputFormat.setInputPaths(job, inPath); 
		 
		 job.setMapperClass(SumFlowMapper.class);
		 job.setMapOutputKeyClass(PairWritable.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 
		 //shuffle过程 
	     job.setPartitionerClass(NamePartitioner.class);
		 job.setGroupingComparatorClass(NameGroup.class);
		 
		 job.setReducerClass(SumFlowReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 
		 Path outPath = new Path(args[1]);
		 FileSystem fs = outPath.getFileSystem(conf);//本地
		// FileSystem fs = FileSystem.get(conf);//集群
		 if(fs.exists(outPath)){
			 fs.delete(outPath, true);
		 }
		 FileOutputFormat.setOutputPath(job,outPath);
		 
		 
		 boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://ns1/input/bb.txt",
				"hdfs://ns1/output"
//				"C:/Users/lx/Desktop/cc.txt",
//				"C:/output"
		};
		int status = ToolRunner.run(conf,  new SumFlowMapReduce(), args);
		System.out.print(status);
		System.exit(status);
	}
	

}
