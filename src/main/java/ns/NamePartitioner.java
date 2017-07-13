package ns;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NamePartitioner extends Partitioner<PairWritable,IntWritable>{

	@Override
	public int getPartition(PairWritable key, IntWritable value,
			int numPartitions) {
		System.out.println("fenqu");
		return (key.getName().hashCode() & Integer.MAX_VALUE)% numPartitions;
	}

	
}
