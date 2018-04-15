package harry.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import harry.hadoop.writable.CityWritable;

/**
 * 
 * @author harry
 *
 */
public class CityMapper extends Mapper<LongWritable, Text, CityWritable, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CityWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] arrs = value.toString().split(" ");
		String cityName = arrs[1];
		context.write(new CityWritable(Integer.parseInt(arrs[0]), cityName, Float.parseFloat(arrs[2])), NullWritable.get());
	}
}
