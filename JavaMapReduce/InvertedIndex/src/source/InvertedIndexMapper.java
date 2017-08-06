package source;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndexMapper extends Mapper<Text,Text,Text,Text>{
	
	
	@Override
	public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] words=line.split("\\W");	
		if(words.length>0){
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			StringBuffer output = new StringBuffer();
			output.append(fileName).append("@").append(key);
			for(String word: words){
				context.write(new Text(word.toLowerCase()), new Text(output.toString()));
			}
		}
		
	}

}