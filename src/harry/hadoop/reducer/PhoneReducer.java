package harry.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import harry.hadoop.writable.PhoneWritable;

/**
 * 
 * @author harry
 *
 */
public class PhoneReducer extends Reducer<Text, PhoneWritable, PhoneWritable, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<PhoneWritable> values,
			Reducer<Text, PhoneWritable, PhoneWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		long totalUp = 0;
		long totalDown = 0;
		for (PhoneWritable phoneWritable : values) {
			totalUp += phoneWritable.getUp();
			totalDown += phoneWritable.getDown();
		}
		
		context.write(new PhoneWritable(key.toString(), totalUp, totalDown), NullWritable.get());
	}
}
