package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import myhadoop.mappers.DepartureDelayCountMapper;
import myhadoop.reducers.DelayCountReducer;

public class DepartureDelayCount {

	public static void main(String[] args) throws Exception{
		//입출력 데이터 경로 확인
		if (args.length !=2) {
			System.err.println("Usage : DepartureDelayCount <input> <output>");
			System.exit(2);
		}
		
		//하둡 설정 로드
		Configuration conf = new Configuration();
		
		//job 이름 설정
		Job job = Job.getInstance(conf, "DepartureDelayCount");
		
		//입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 잡, 매퍼, 리듀서 등록
		job.setJarByClass(DepartureDelayCount.class);
		job.setMapperClass(DepartureDelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		//입출력 데이터 포맷 결정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//출력 키, 값 유형 설정
		job.setOutputKeyClass(Text.class);// 년,월
		job.setOutputValueClass(IntWritable.class);
		
		//Job 실행
		job.waitForCompletion(true);
		
	}

}
