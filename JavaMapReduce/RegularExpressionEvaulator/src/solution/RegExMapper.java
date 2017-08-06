package solution;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RegExMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static IntWritable ONE = new IntWritable(1);
	private static Text MATCHED = new Text("match");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String regexPattern=conf.get("regex");
		  
	    String line = value.toString();
	    Pattern regex = Pattern.compile(regexPattern);
		String[] words = regex.split(line);
		
	    for (String word : words) {
	        context.write(MATCHED, ONE);
	    }
	}
}
