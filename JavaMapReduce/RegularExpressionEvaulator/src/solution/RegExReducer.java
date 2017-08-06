package solution;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegExReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
  long overallCount = 0;
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    long count = 0;
	    for (IntWritable value : values) {
	        count += value.get();
	        overallCount++;
	    }
	    if (overallCount > 0) {
	       	context.write(key, new DoubleWritable(overallCount));
	    }  
  }
}
