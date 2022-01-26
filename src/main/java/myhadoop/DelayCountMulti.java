package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DelayCountMultimapper;
import myhadoop.reducers.DelayCountMultiReducer;

public class DelayCountMulti {

	public static void main(String[] args) throws Exception{
		//입출력 데이터 정보 확인
		if(args.length !=2) {
			System.err.println("Usage : DelayCountMulti <input><output>zzzennn");
			System.exit(2);
		}
		Configuration conf = new Configuration();// 설정 불러오기
		
		//Job생성
		Job job = Job.getInstance(conf, "DelayCountMulti");
		
		//입출력 경로 생성
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 잡과 맵과 리듀스 클래스를 등록
		job.setJarByClass(DelayCountMulti.class);
		job.setMapperClass(DelayCountMultimapper.class);
		job.setReducerClass(DelayCountMultiReducer.class);
		
		//입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//출력 키, 값 타입 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//다중출력 namdedOutput 만들기
		MultipleOutputs.addNamedOutput(job, 
				"departure", // 아웃풋 이름 
				TextOutputFormat.class, // 아웃풋 출력 형식
				Text.class, // 아웃풋 키타입
				IntWritable.class); // 아웃풋 값타입
		MultipleOutputs.addNamedOutput(job, 
				"arrival", // 
				TextOutputFormat.class,
				Text.class,
				IntWritable.class);
				
		job.waitForCompletion(true);
		
		
		
	}

}
