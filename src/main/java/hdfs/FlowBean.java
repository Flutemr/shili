package hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * Writable 是Hadoop的序列化接口，自定义value要实现这个接口
 * @author lx
 *
 */

public class FlowBean implements Writable {
	private long upflow;
	private long downflow;
	private long sumflow;
	private int uppackage;
	private int downpackage;
	private int sumpackage;
	
	

	public FlowBean(long upflow, long downflow, long sumflow, int uppackage,
			int downpackage, int sumpackage) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = sumflow;
		this.uppackage = uppackage;
		this.downpackage = downpackage;
		this.sumpackage = sumpackage;
	}

	public int getUppackage() {
		return uppackage;
	}

	public void setUppackage(int uppackage) {
		this.uppackage = uppackage;
	}

	public int getDownpackage() {
		return downpackage;
	}

	public void setDownpackage(int downpackage) {
		this.downpackage = downpackage;
	}

	public int getSumpackage() {
		return sumpackage;
	}

	public void setSumpackage(int sumpackage) {
		this.sumpackage = sumpackage;
	}

	public FlowBean() {
	}

	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	public long getDownflow() {
		return downflow;
	}

	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}

	public long getSumflow() {
		return sumflow;
	}

	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}
//序列化   便于在网络中传输，（将对象转换成二进制流）
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow);
		out.writeLong(downflow);
		out.writeLong(sumflow);
		out.writeInt(uppackage);
		out.writeInt(downpackage);
		out.writeInt(sumpackage);
	}
//反序列化  讲二进制流转换成对象
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.sumflow = in.readLong();
		this.uppackage = in.readInt();
		this.downpackage = in.readInt();
		this.sumpackage = in.readInt();
	}

	@Override
	public String toString() {
		return "upflow=" + upflow + ", downflow=" + downflow
				+ ", sumflow=" + sumflow + ", uppackage=" + uppackage
				+ ", downpackage=" + downpackage + ", sumpackage=" + sumpackage
				;
	}

	

}
