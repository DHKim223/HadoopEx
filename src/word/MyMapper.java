package word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//1, read a book
	//2, write a book
	
	// <read,1> <a,1><book,1><write,1><a,1><book,1>
			// 안바뀜 여러가안됨
	private final static IntWritable one = new IntWritable(1);	// out value
	private Text word = new Text(); // out key
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString());
			while(st.hasMoreTokens()) {
				word.set(st.nextToken());			// read, a ,book
				context.write(word, one);			//<read,1> <a,1> <book,1>
			}
			
	}
}
