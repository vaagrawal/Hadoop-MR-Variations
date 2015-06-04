package hadoop101.Union;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnionReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,
			Reducer<IntWritable, Text, IntWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String names = "";
		for (Text name : arg1) {
			String newname = name.toString();
			if (!names.contains(newname)) {
				names=names.concat(newname);
			}
		}
		arg2.write(arg0, new Text(names));
	}

}
