package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.WordCountMapper;
import myhadoop.reducers.WordCountReducer;
//import myhadoop.reducers.WordCountReducer;

public class WordCount {
//		1. 잡 객체 생성
//		2. 맵리듀스 잡에 필요한 실행 정보를 지정
//		3. 맵리듀스 잡을 실행
//		WordCount <원본> <output>

	public static void main(String[] args) throws Exception {
		// 매개변수 체크
		if (args.length !=2) {
			System.err.println("Usage:WordCount <input> <output>");
			System.exit(2);
			
		}
		
		Configuration conf = new Configuration(); // 설정 불러오기
		
		Job job = Job.getInstance(conf, "WordCount"); 
		// 내부에서 처리가 곤란한 건 바깥으로 던지려고 throws가 나옴 (예외 발생시 종료해버리기로)
		job.setJarByClass(WordCount.class);// 드라이버 클래스 등록
		job.setMapperClass(WordCountMapper.class);//매퍼 등록
		job.setReducerClass(WordCountReducer.class); // 리듀서 등록

		// 입출력 파일의 포맷 등록
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//출력키, 값의 타입 등록
		// (단어, 집계값)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//입출력 위치 정보 설정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//하둡 전체적인 잡 실행
		job.waitForCompletion(true);
		
	}

}
