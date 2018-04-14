package harry.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author harry
 *
 */
public class PhoneWritable implements WritableComparable<PhoneWritable> {
	private String phone;
	private Long up;
	private Long down;
	private Long total;
	
	public PhoneWritable() {
		
	}
	
	public PhoneWritable(String phone,Long up,Long down) {
		this.phone = phone;
		this.up = up;
		this.down = down;
		this.total = up + down;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(getPhone());
		out.writeLong(getUp());
		out.writeLong(getDown());
		out.writeLong(getTotal());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setPhone(in.readUTF());
		setUp(in.readLong());
		setDown(in.readLong());
		setTotal(in.readLong());
	}

	@Override
	public int compareTo(PhoneWritable o) {
		if(this.getTotal() != o.getTotal()){
			if(this.getTotal() > o.getTotal()){
				return 1;
			}else{
				return -1;
			}
		}
		
		return this.getPhone().compareTo(o.getPhone());
	}
	
	@Override
	public String toString() {
		return this.getPhone() + "\t" + this.getUp() + "\t" + this.getDown() + "\t" + this.getTotal();
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Long getUp() {
		return up;
	}

	public void setUp(Long up) {
		this.up = up;
	}

	public Long getDown() {
		return down;
	}

	public void setDown(Long down) {
		this.down = down;
	}

	public Long getTotal() {
		return total;
	}

	public void setTotal(Long total) {
		this.total = total;
	}

}
