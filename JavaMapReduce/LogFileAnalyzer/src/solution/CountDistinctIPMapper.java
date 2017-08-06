package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountDistinctIPMapper extends
		Mapper<LongWritable, Text, CountDistinctIPKey, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split("\\s+");
		if (words.length > 0) {
			String ip = words[0];
			String month = words[3].split("/")[1];
			context.write(new CountDistinctIPKey(ip, month), ONE);
		}

	}

}