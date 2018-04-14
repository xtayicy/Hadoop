package harry.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import harry.hadoop.mapper.KeyValueTextInputFormatMapper;
import harry.hadoop.mapper.PhoneMapper;
import harry.hadoop.mapper.TextInputFormatMapper;
import harry.hadoop.reducer.PhoneReducer;
import harry.hadoop.writable.PhoneWritable;

/**
 * 
 * @author harry
 *
 */
public class JobCenter {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		//configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",">");
		
		Job job = Job.getInstance(configuration);
		job.setJarByClass(JobCenter.class);
		
		setInputByInputFormat(job);
		setOutput(configuration, job);
		
		//job.setMapperClass(WordMapper.class);
		//job.setReducerClass(WordReducer.class);
		
		job.setMapperClass(PhoneMapper.class);
		job.setReducerClass(PhoneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PhoneWritable.class);
		job.setOutputKeyClass(PhoneWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.waitForCompletion(true);
	}

	private static void setOutput(Configuration configuration, Job job) throws IOException {
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path("output");
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
		}
		
		TextOutputFormat.setOutputPath(job, outputPath );
	}
	
	private static void setInputByInputFormat(Job job) throws IOException{
		job.setInputFormatClass(TextInputFormat.class);
		Path inputPath = new Path("input/phone");
		TextInputFormat.addInputPath(job, inputPath);
	}
	
	private static void setInputByKeyValueTextInputForat(Job job) throws IOException{
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		Path inputPath = new Path("input");
		TextInputFormat.addInputPath(job, inputPath);
	}

	private static void setInputByMultipleInputs(Job job) throws IOException {
		MultipleInputs.addInputPath(job, new Path("input/data1"), KeyValueTextInputFormat.class, KeyValueTextInputFormatMapper.class);
		MultipleInputs.addInputPath(job, new Path("input/data2"), TextInputFormat.class, TextInputFormatMapper.class);
	}
}
