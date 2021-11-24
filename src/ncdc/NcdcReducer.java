package ncdc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NcdcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context) 
			throws IOException, InterruptedException {
		
		int max = Integer.MIN_VALUE;
		for (IntWritable v : value) {
			max = Math.max(max, v.get());
		}
		context.write(key,new IntWritable(max));
	}
}
