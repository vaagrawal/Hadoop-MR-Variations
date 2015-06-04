package hadoop101.UniqueCount;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] sections=line.split(",");
		String user = sections[0];
		String website = sections[1];
		//Pass the website as the key and the whole line as the value.
		context.write(new Text(website), value);
	}
}
