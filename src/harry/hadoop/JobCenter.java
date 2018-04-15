package harry.hadoop;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import harry.hadoop.comparator.CityComparator;
import harry.hadoop.groupComparator.CityGroupComparator;
import harry.hadoop.mapper.CityMapper;
import harry.hadoop.mapper.KeyValueTextInputFormatMapper;
import harry.hadoop.mapper.PhoneMapper;
import harry.hadoop.mapper.TextInputFormatMapper;
import harry.hadoop.mapper.WordMapper;
import harry.hadoop.reducer.CityReducer;
import harry.hadoop.reducer.PhoneReducer;
import harry.hadoop.reducer.WordReducer;
import harry.hadoop.writable.CityWritable;
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
		
		setInputByTextInputFormat(job);
		setOutputBySequenceFileOutputFormat(configuration, job);
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		
		//job.setMapperClass(PhoneMapper.class);
		//job.setReducerClass(PhoneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PhoneWritable.class);
		job.setOutputKeyClass(PhoneWritable.class);
		job.setOutputValueClass(NullWritable.class);*/
		
		/*job.setMapOutputKeyClass(CityWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(CityWritable.class);
		job.setOutputValueClass(NullWritable.class);*/
		
		//job.setPartitionerClass(YearPartitioner.class);
		//job.setPartitionerClass(HashPartitioner.class);
		/*job.setPartitionerClass(new Partitioner<Text,IntWritable>(){

			@Override
			public int getPartition(Text text, IntWritable value, int numReducerTask) {
				return new Random().nextInt(3);
			}
			
		}.getClass());*/
		
		//job.setNumReduceTasks(3);
		//job.setCombinerClass(CityReducer.class);
		//job.setSortComparatorClass(CityComparator.class);
		//job.setGroupingComparatorClass(CityGroupComparator.class);
		job.waitForCompletion(true);
	}

	private static void setOutput(Configuration configuration, Job job) throws IOException {
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path("output/data2");
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
		}
		
		TextOutputFormat.setOutputPath(job, outputPath );
	}
	
	private static void setOutputBySequenceFileOutputFormat(Configuration configuration, Job job) throws IOException {
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		Path outputPath = new Path("output/data2");
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
		}
		
		TextOutputFormat.setOutputPath(job, outputPath );
	}
	
	private static void setInputByTextInputFormat(Job job) throws IOException{
		job.setInputFormatClass(TextInputFormat.class);
		Path inputPath = new Path("input/data2");
		TextInputFormat.addInputPath(job, inputPath);
	}
	
	private static void setInputByKeyValueTextInputFormat(Job job) throws IOException{
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		Path inputPath = new Path("input/data1");
		TextInputFormat.addInputPath(job, inputPath);
	}

	private static void setInputByMultipleInputs(Job job) throws IOException {
		MultipleInputs.addInputPath(job, new Path("input/data1"), KeyValueTextInputFormat.class, KeyValueTextInputFormatMapper.class);
		MultipleInputs.addInputPath(job, new Path("input/data2"), TextInputFormat.class, TextInputFormatMapper.class);
	}
}
