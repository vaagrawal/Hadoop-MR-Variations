package hadoop101.Union;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UnionConfig extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(UnionConfig.class);
		job.setJobName(this.getClass().getName());

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, UnionMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, UnionMapper1.class);

		FileInputFormat
				.setInputPaths(job, new Path(args[0], new Path(args[1])));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(UnionMapper1.class);
		// job.setMapperClass(MultiplePathsMapper2.class);
		job.setReducerClass(UnionReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
		if (job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UnionConfig(), args);
		System.exit(exitCode);
	}
}