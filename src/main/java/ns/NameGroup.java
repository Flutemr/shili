package ns;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

	public class NameGroup implements RawComparator<PairWritable>{
		
	public int compare(PairWritable o1,PairWritable o2){
		System.out.println("fenzu");
		return o1.getName().compareTo(o2.getName());
	}
	public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
		System.out.println("分组");
		return WritableComparator.compareBytes(b1,0, l1-4, b2, s2, l2-4);
	}
}
