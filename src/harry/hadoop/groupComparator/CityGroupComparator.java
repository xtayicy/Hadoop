package harry.hadoop.groupComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import harry.hadoop.writable.CityWritable;

/**
 * 
 * @author harry
 *
 */
public class CityGroupComparator extends WritableComparator {
	public CityGroupComparator(){
		super(CityWritable.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CityWritable city_1 = (CityWritable)a;
		CityWritable city_2 = (CityWritable) b;
		
		return city_1.getYear().compareTo(city_2.getYear());
	}
}
