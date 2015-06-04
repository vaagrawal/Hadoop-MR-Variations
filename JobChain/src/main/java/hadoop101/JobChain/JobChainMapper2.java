package hadoop101.JobChain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobChainMapper2 extends
		Mapper<IntWritable, Text, IntWritable, Text> {
	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] ids = line.split(",");
		
		String finalvalue = ids[1]+ids[2]+ids[3]+ids[4];
		if (Integer.parseInt(ids[4])>50000) {
			context.write(new IntWritable(Integer.parseInt(ids[0])), new Text(finalvalue));
		}
	}

}
