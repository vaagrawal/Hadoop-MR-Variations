package hadoop101.JobChain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobChainConfig extends Configured implements Tool{
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Configuration conf = new Configuration();
		Job job1 = new Job(conf);
		job1.setJarByClass(JobChainConfig.class);
		job1.setJobName("First Job");
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		//Output to a temp path in root/tmp/
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(JobChainMapper1.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		job1.waitForCompletion(true);

		
		//Job2
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2);
		job2.setJarByClass(JobChainConfig.class);
		job2.setJobName("Second Job");
		
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(JobChainMapper2.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		  
		if (job2.waitForCompletion(true)) {
			return 0;
		}
		return 1;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JobChainConfig(), args);
		System.exit(exitCode);
	}


}
