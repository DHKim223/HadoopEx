package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFile {

	public static void main(String[] args) {
		// 입력 파라미터 확인
		if( args.length != 2 ) {
			System.out.println("사용방법 : HdfsFile <filename> <contents>");
			System.exit(2); // 비정상 종료,,, 매개변수 2개가 아니면 의미가없어서 비정상 종료.
		}
		
		// 파일시스템 설정
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf) ;
			
			Path path = new Path(args[0]);
			if (fs.exists(path)) {
				fs.delete(path, true);
			} 
			
			// HDFS 파일로 저장
			FSDataOutputStream os = fs.create(path);
			os.writeUTF(args[1]);
			os.close();
			
			// HDFS 파일 읽기 -> 콘솔로 출력
			FSDataInputStream is = fs.open(path);
			String contents = is.readUTF();
			is.close();
			System.out.println("contents : " + contents);
			
		} catch (IOException e ) {
			e.printStackTrace();
		}
	}
}
