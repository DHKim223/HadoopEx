package word;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable > {
	//<read,1> <a,1><book,1><write,1><a,1><book,1>
	//<read,1> <a,2> <book,2> <write,1>
	private IntWritable result = new IntWritable();
	
	@Override
	//<read,(1)> <a,(1,1)><book,(1,1)><write,(1)>
	protected void reduce(Text key, Iterable<IntWritable> value, Context context ) 
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : value ) {
			sum += v.get();			
		}
		result.set(sum);
		context.write(key, result);			// <read,1> <a,2> <book ,2> <write ,1 >				
	}
	
}
