package solution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountDistinctIPPartitioner extends Partitioner<CountDistinctIPKey, IntWritable>{

	@Override
	public int getPartition(CountDistinctIPKey key, IntWritable value, int numPartitions) {
		
		String month = key.getmonth();
		
		if(month.equals("Jan")){
			return 0;
		}	
		if(month.equals("Feb")){
			return 1;
		}
		if(month.equals("Mar")){
			return 2;
		}
		if(month.equals("Apr")){
			return 3;
		}
		if(month.equals("May")){
			return 4;
		}
		if(month.equals("Jun")){
			return 5;
		}	
		if(month.equals("Jul")){
			return 6;
		}
		if(month.equals("Aug")){
			return 7;
		}
		if(month.equals("Sep")){
			return 8;
		}
		if(month.equals("Oct")){
			return 9;
		}
		if(month.equals("Nov")){
			return 10;
		}
		if(month.equals("Dec")){
			return 10;
		}
		
		return -1;
		
	}

}