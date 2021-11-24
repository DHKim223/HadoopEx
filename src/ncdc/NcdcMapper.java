package ncdc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NcdcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// 없는데이터 제거
	private static final int missing = 9999;
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = null;
		if(line != null) {
			year = line.substring(15, 19);
		}
		int temperature = 0;
		if(line.charAt(87) == '+' ) {
			temperature = Integer.parseInt(line.substring(88 , 92));	//88~91
		} else {
			temperature = Integer.parseInt(line.substring(87 , 92));//97~91
		}
		String quality = line.substring(92 , 93);
		if(temperature != missing & quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(temperature));
		}
	}
}
