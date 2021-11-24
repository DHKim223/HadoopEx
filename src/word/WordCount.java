package word;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if (args.length != 2) {
			System.out.println("Usage : WordCount <input> <output>");
			System.exit(2);
		}
		Configuration conf = new Configuration();		// 하둡에있는 경로를 가져오기 위해서 환경설정값들을 가져온다.
		Job job = Job.getInstance(conf, "wordcount"); // 실제로 분석을하면 job을 할당 // 작업 하나....
		job.setJarByClass(WordCount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		// 입출력 데이터 형식 지정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
