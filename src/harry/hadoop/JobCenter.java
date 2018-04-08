package harry.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import harry.hadoop.mapper.WordMapper;
import harry.hadoop.reducer.WordReducer;

/**
 * 
 * @author harry
 *
 */
public class JobCenter {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",">");
		
		Job job = Job.getInstance(configuration);
		job.setJarByClass(JobCenter.class);
		
		setInput(job);
		setOutput(configuration, job);
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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

	private static void setInput(Job job) throws IOException {
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		Path inputPath = new Path("input");
		TextInputFormat.addInputPath(job, inputPath );
	}
}
