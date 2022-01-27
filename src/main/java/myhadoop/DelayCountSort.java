package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DelayCountSortMapper;
import myhadoop.reducers.DelayCountSortReducer;
import myhadoop.support.DateKey;
import myhadoop.support.DateKeyComparator;
import myhadoop.support.GroupKeyComparator;
import myhadoop.support.GroupKeyPartitioner;

public class DelayCountSort {

	public static void main(String[] args) throws Exception {
		//입출력 데이터 경로 확인
		if (args.length != 2) {
			System.err.println("Usage : DelayCountSort <input><output>");
			System.exit(2);
			
		}
		
		//설정 불러오기
		Configuration conf = new Configuration();
		
		//잡 설정, 여기서 빨간줄이 생기니까 메인 구문에 throws를 삽입해주는것임
		Job job = Job.getInstance(conf, "DelayCountSort");
		
		//입출력 경로
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//잡, 매퍼, 리듀서 등록
		job.setJarByClass(DelayCountSort.class);
		//파티셔너 클래스 등록
		job.setPartitionerClass(GroupKeyPartitioner.class);
		//그룹 키 비교기 등록
		job.setCombinerKeyGroupingComparatorClass(GroupKeyComparator.class);
		//소팅을 위한 복합키 비교기 등록
		job.setSortComparatorClass(DateKeyComparator.class);
		
		job.setMapperClass(DelayCountSortMapper.class);
		job.setReducerClass(DelayCountSortReducer.class);
		
		//매퍼 출력 키, 값의 타입 결정
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//리듀서 출력키와 값의 타입
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		//입출력 데이터 포맷
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//다중 출력 설정 : departure, arrival
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);
		
		job.waitForCompletion(true);
		
		
	}

}
