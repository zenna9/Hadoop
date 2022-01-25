package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsWrite {
	//HdfsWrite <filename> <contents>
	public static void main(String[] args) {
		//입력 파라미터 점검
		if (args.length <=2) { // 파라미터가 모자라단 뜻
			System.err.println("Usage : HdfsWrite <filename> <contents>");
			System.exit(2);
		}
		
		try {
			//하둡 설정 불러오기
			Configuration conf = new Configuration();
			//HDFS 파일시스템 불러오기
			FileSystem fs = FileSystem.get(conf);
			
			//저장 경로 체크 : args[0]
			Path path = new Path(args[0]);
			
			//저장 경로가 이미 있으면 overwrite가 안되니까 지워주기로
			if (fs.exists(path)) { // 저장경로에 path가 이미 있다면
				fs.delete(path, true); // fs에서 path를 지워주되, 리커시브로 지우는거 true
			}
			
			// 컨텐츠를 불러와서 args[1] ~args[args.lengh -1 ] 
			FSDataOutputStream os = fs.create(path);
			
			//컨텐츠 데이터를 루프 돌며 읽어오기 
			for (int i =1 ; i< args.length ; i++) {
				//아웃풋 스트림에 기록
				os.writeUTF(args[i] + " ");
			}
			//개행 출력
			os.writeUTF("\n");
			os.close();
			
			System.out.println("메시지가 저장되었습니다");
		} catch (IOException e) {
			System.err.println("파일 저장 에러!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}}

