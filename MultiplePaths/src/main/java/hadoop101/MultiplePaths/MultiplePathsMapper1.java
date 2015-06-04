package hadoop101.MultiplePaths;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//Each Mapper does only filters the rows based on the states
public class MultiplePathsMapper1 extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String lines = value.toString();
		String words[] = lines.split(",");
		if(words[1].equals("AZ"))
		{
			context.write(new Text(words[0]), value);
		}
		
	}
}
