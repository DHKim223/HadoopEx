package airplane;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text outputkey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		
			DelayParser parser = new DelayParser(key,value);
			// 도착지연    //outputkey.set(parser.getYear() + "년 " + parser.getMonth()+"월");		// 키 벨류는 1,,,
			if( parser.getUniqueCarrier() != null )
				outputkey.set( parser.getUniqueCarrier() );
			
			if( parser.getArriveDelay() > 0 ) {
		// if( parser.isDepartureDelayAvailable() ) {				// 출발 지연
					context.write( outputkey, one );					// <2008년 1월, 1>
			}
			/*
			if( parser.getDepartureDelay() > 0 ) {
				context.write(outputkey, one);
			}
			*/
			
			
	}
	
}
