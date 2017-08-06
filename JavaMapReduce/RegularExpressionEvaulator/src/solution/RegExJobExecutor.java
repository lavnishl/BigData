package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RegExJobExecutor extends Configured implements Tool {
	public static int ERROR = -1;
	public static int SUCCESS = 0;

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err
					.println("please enter 3 inputs in following order : <input_folder> <output_folder> <reg_expression>");
			System.exit(ERROR);
		}
		int exitCode = ToolRunner.run(conf, new RegExJobExecutor(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("regex", args[2]);
		Job job = Job.getInstance(conf, "RegEx JobExecutor");
		job.setJar("RegExJobExecutor.jar");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RegExMapper.class);
		job.setReducerClass(RegExReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? SUCCESS : ERROR;
	}


}
