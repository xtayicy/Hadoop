package harry.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import harry.hadoop.writable.PhoneWritable;
import javafx.scene.shape.VLineTo;

/**
 * 
 * @author harry
 *
 */
public class PhoneMapper extends Mapper<LongWritable, Text, Text, PhoneWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneWritable>.Context context)
			throws IOException, InterruptedException {
		String[] arrs = value.toString().split(" ");
		String phone = arrs[0];
		Long up = Long.parseLong(arrs[2]);
		Long down = Long.parseLong(arrs[3]);
		context.write(new Text(phone), new PhoneWritable(phone,up,down));
	}
}
