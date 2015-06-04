package hadoop1.MR1Wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	int counter =0;
	for(IntWritable count: arg1){
		counter++;
	}
		arg2.write(arg0,new IntWritable(counter));
		
	}
}
