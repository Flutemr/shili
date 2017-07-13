package work;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NamePartition extends Partitioner<PairWritable, IntWritable> {

	@Override
	public int getPartition(PairWritable key, IntWritable value,
			int numPartitions) {
		return  (key.getName().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	
 

}
