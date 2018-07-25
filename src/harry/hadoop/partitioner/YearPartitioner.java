package harry.hadoop.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import harry.hadoop.writable.CityWritable;

/**
 * 
 * @author harry
 *
 */
public class YearPartitioner extends Partitioner<CityWritable, Text> {

	@Override
	public int getPartition(CityWritable cityWritable, Text text, int numPartitions) {
		return (cityWritable.getYear().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
