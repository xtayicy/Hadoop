package harry.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author harry
 *
 */
public class KeyValueTextInputFormatMapper extends Mapper<Text, Text, Text,IntWritable>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable one = new IntWritable(1);
		for (String word : value.toString().split(" ")) {
			context.write(new Text(word), one);
		}
	}
}
