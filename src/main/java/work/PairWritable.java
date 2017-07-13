package work;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {
	private String name;
	private int money;
	
	
	public PairWritable(String name, int money) {
		this.name = name;
		this.money = money;
	}

	public PairWritable() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getMoney() {
		return money;
	}

	public void setMoney(int money) {
		this.money = money;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(money);
	}

	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.money = in.readInt();
	}

	public int compareTo(PairWritable o) {
		int comp = this.name.compareTo(o.getName());
		if(0 != comp){
			return comp;
		}
		return (Integer.valueOf(this.money)).compareTo(Integer.valueOf(o.getMoney()));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + money;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritable other = (PairWritable) obj;
		if (money != other.money)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "name=" + name + ", money=" + money ;
	}

	public void set(String name2, int money2) {
		this.name = name2;
		this.money = money2;
	}
	
}
