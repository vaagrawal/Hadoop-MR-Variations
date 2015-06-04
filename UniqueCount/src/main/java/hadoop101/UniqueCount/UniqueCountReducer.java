package hadoop101.UniqueCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueCountReducer extends Reducer<Text, Text, Text, IntWritable> {
	
	
	private String names;

	protected void reduce(Text arg0, Iterable<Text> arg1,
			Reducer<Text, Text, Text, IntWritable>.Context arg2) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		names = "";
		for(Text name: arg1){
			String newname = name.toString();
			if(!names.contains(newname)){
				names.concat(newname);
				count++;
			}
		}
		
//		arg2.write(arg0, new Text(names));
	
		//Output the website fromt the key and count from above.
		//This code could also be modified to print all the users that had accessed the website.
		arg2.write(arg0, new IntWritable(count));
	}

}
