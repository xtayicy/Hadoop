package harry.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import harry.hadoop.writable.CityWritable;

/**
 * 
 * @author harry
 *
 */

public class CityReducer extends Reducer<CityWritable, NullWritable, CityWritable, NullWritable> {
	@Override
	protected void reduce(CityWritable cityWritable, Iterable<NullWritable> values,
			Reducer<CityWritable, NullWritable, CityWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(cityWritable, NullWritable.get());
	}
}
