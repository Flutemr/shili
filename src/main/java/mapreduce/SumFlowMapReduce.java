package mapreduce;

import hdfs.FlowBean;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public  class SumFlowMapReduce extends Configured implements Tool {
	
	
	public static class SumFlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
		//private Text mapOutputKey = new Text();
	//	private IntWritable mapOutputValue = new IntWritable(1);
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		//	System.out.println(key.toString());
		//	System.out.println(value.toString());
			String line = value.toString();
			String[] strs = line.split("\t");
			//String str = line.substring(line.indexOf("@"));
			String phoneNM = strs[1];//取手机号
			int uppackage = Integer.parseInt(strs[6]);
			System.out.println(uppackage);
			int downpackage =Integer.parseInt(strs[7]);
			int sumpacakge = uppackage + downpackage;
			long upflow = Long.parseLong(strs[8]);
			long downflow = Long.parseLong(strs[9]);
			long sumflow = upflow + downflow;
			context.write(new Text(phoneNM),new FlowBean(upflow, downflow, sumflow,uppackage,downpackage,sumpacakge));
			/*for(String str:strs){
				mapOutputKey.set(str);
				context.write(mapOutputKey, mapOutputValue);
			}*/
		}
	}
	public static class SumFlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,
				Context context)
				throws IOException, InterruptedException {
			long upflowsum = 0;
			long downflowsum = 0;
			long sumflow = 0;
			int uppackage = 0;
			int downpackage = 0;
			int sumpacakge = 0;
			for(FlowBean value:values){
				System.out.println(value);
				upflowsum += value.getUpflow();
				System.out.println(upflowsum);
				downflowsum += value.getDownflow();
				uppackage += value.getUppackage();
				System.out.println(uppackage);
				downpackage += value.getDownpackage();
			}
			sumflow = upflowsum +downflowsum;
			sumpacakge = uppackage + downpackage;
	    context.write(key, new FlowBean(upflowsum, downflowsum, sumflow,uppackage,downpackage,sumpacakge));
		}
		
	}
	public static class MyPartitioner extends Partitioner<Text,FlowBean>{
		public static HashMap<String,Integer> provinceIDs = new HashMap<String,Integer>();
		static{
			provinceIDs.put("134", 0);
			provinceIDs.put("135", 1);
			provinceIDs.put("136", 2);
			provinceIDs.put("137", 3);
			provinceIDs.put("159", 4);
		}
		@Override
		public int getPartition(Text key, FlowBean value, int numPartitions) {
			String phone = key.toString().substring(0, 3);
			Integer provinceID = provinceIDs.get(phone);
			
			return provinceID == null ? 5 : provinceID;
		}
		
	}
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, this.getClass().getName());
		 job.setJarByClass(this.getClass());
		 
		 Path inPath = new Path(args[0]);
		 FileInputFormat.setInputPaths(job, inPath); 
		 
		 job.setMapperClass(SumFlowMapper.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(FlowBean.class);
		 
		 //shuffle过程 
		 job.setPartitionerClass(MyPartitioner.class);
		 job.setNumReduceTasks(6);
		 
		 job.setReducerClass(SumFlowReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(FlowBean.class);
		 
		 
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
/*		args = new String[]{
				"hdfs://zhuyun.com:8020/input/HTTP_20130313143750.data",
				"hdfs://zhuyun.com:8020/output"
//				"C:/Users/lx/Desktop/cc.txt",
//				"C:/output"
		};*/
		int status = ToolRunner.run(conf,  new SumFlowMapReduce(), args);
		System.out.print(status);
		System.exit(status);
	}
	

}
