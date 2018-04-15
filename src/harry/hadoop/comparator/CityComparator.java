package harry.hadoop.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import harry.hadoop.writable.CityWritable;

/**
 * 
 * @author harry
 *
 */
public class CityComparator extends WritableComparator {
	public CityComparator(){
		super(CityWritable.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CityWritable city_1 = (CityWritable)a;
		CityWritable city_2 = (CityWritable) b;
		if(city_1.getYear().compareTo(city_2.getYear()) != 0){
			return city_1.getYear().compareTo(city_2.getYear());
		}else if(city_1.getTempreture().compareTo(city_2.getTempreture()) != 0){
			return city_1.getTempreture().compareTo(city_2.getTempreture());
		}
		
		return city_1.getCityName().compareTo(city_2.getCityName());
	}
}
