package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSListFiles {

	public static void main(String[] args) {
		if(args.length !=1) { //인자로 <dir>안넘어옴
			System.err.println("Usage: HDFSListFiles <dir>"); // 사용법 안내
			System.exit(2);
		}
		
		//인자값으로 넘어온 디렉터리를 찾아서 파일(디렉터리)출력
		//설정 불러오기
		Configuration conf = new Configuration();
		
		// 인자로 넘어온 dir을 확보
		String dir = args[0];
		
		try { //예외처리
			FileSystem fs = FileSystem.get(conf);
			RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(dir), true);
		
			while(it.hasNext()) { // 처리할 항목이 더 있는지
				LocatedFileStatus status = it.next(); // 파일 정보 추출
				// 정보 출력
				System.out.printf("%s %s%n", status.isDirectory() ? "d": "f", status.getPath()); // %는 포맷타입 의미
			}
			
		} catch (IOException e) {
			e.printStackTrace(); //예외 정보 출력
		}
	}

}
