package hadoop101.JobChain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobChainMapper1  extends Mapper<LongWritable, Text, IntWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] ids = line.split(",");
		//Filter based on State CA which is in location id[2] based on schema
		if (ids[2].equalsIgnoreCase("CA")) {
			String keyval=ids[0];
			String finalvalue= ","+ids[1]+","+ids[2]+","+ids[3];
			context.write(new IntWritable(Integer.parseInt(keyval.trim())), new Text(finalvalue));
			//This now adds another field to the intermediate file
		}
	}

}
