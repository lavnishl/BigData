package source;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{
	
	@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		StringBuffer buffer= new StringBuffer();
		for(Text value: values){
			buffer.append(value);
			buffer.append(",");
		}
		
		context.write(key, new Text(buffer.toString().substring(0, buffer.length()-1)));
		
	}

}