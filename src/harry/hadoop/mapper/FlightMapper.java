package harry.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author harry
 *
 */
public class FlightMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().split(" ");
		String fromCity = strs[0];
		String toCity = strs[1];
		String time = strs[2];
		String duration = strs[3];
		context.write(new Text(fromCity + "\t" + toCity + "\t" + time + "\t" + duration), NullWritable.get());
	}
}
