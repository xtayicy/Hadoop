package harry.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author harry
 *
 */
public class CityWritable implements WritableComparable<CityWritable> {
	private IntWritable year = new IntWritable();
	private Text cityName = new Text();
	private FloatWritable tempreture = new FloatWritable();
	
	public CityWritable() {
		// TODO Auto-generated constructor stub
	}
	
	public CityWritable(Integer year,String cityName,Float tempreture) {
		this.getYear().set(year);
		this.getCityName().set(cityName);
		this.getTempreture().set(tempreture);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.getYear().write(out);
		this.getCityName().write(out);
		this.getTempreture().write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.getYear().readFields(in);
		this.getCityName().readFields(in);
		this.getTempreture().readFields(in);
	}

	@Override
	public int compareTo(CityWritable o) {
		if(this.getYear().compareTo(o.getYear()) != 0){
			return this.getYear().compareTo(o.getYear());
		}else if(this.getTempreture().compareTo(o.getTempreture()) != 0){
			return this.getTempreture().compareTo(o.getTempreture());
		}
		
		return this.getCityName().compareTo(o.getCityName());
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public Text getCityName() {
		return cityName;
	}

	public void setCityName(Text cityName) {
		this.cityName = cityName;
	}

	public FloatWritable getTempreture() {
		return tempreture;
	}

	public void setTempreture(FloatWritable tempreture) {
		this.tempreture = tempreture;
	}
	
	@Override
	public String toString() {
		return this.getYear() + "\t" + this.getCityName()
				+ "\t" + this.getTempreture();
	}
}
