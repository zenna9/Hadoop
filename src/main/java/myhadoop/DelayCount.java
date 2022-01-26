package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import myhadoop.mappers.DelayCountMapper;
import myhadoop.reducers.DelayCountReducer;

// 환경변수 제어를 위해 Configured클래스 상속받
	//사용자 정의 옵션 제어를 위해 Tool 인터페이스 구현
public class DelayCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new DelayCount(), args);
		System.out.println("MapReduce-Job Result :"+ result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// 전달받은 args 통해 매개변수 분석
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		//- -conf, -fs, -D 같은 하둡의 설정 옵션을 제외한 나머지 인자를 얻어낼 수 있음
		//입출력 경로 확인
		if (otherArgs.length != 2) {
			System.err.println("Usage: DelayCount -DworkType=departure | arrival <input><output>");
			System.exit(2);
		}
		//Job설정해줘야 함
		Job job = Job.getInstance(getConf(),"DelayCount");
		//입출력 경로의 세팅. 인풋과 아웃풋은 otherArgs에 들어가있음
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//드라이버클래서와 매퍼클래스, 리듀서클래스 등록
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		//입출력 데이터 포맷 결정. 양 끝단을 이어줌
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//출력 키, 값 유형 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//드라이버 클래스 실행
		job.waitForCompletion(true);
		
		return 1;
	}



}
