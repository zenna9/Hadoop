package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;
import myhadoop.support.DateKey;

//입력 키 : 라인넘버, 	값 : csv
// 출력키 : DateKey, 	값 : 1
public class DelayCountSortMapper extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	// 맵 출력 값
	private static final IntWritable one= new IntWritable(1);
	private DateKey outputKey = new DateKey();
	
	
	@Override
	protected void map(LongWritable key, 
			Text value,  // csv
			Context context)
			throws IOException, InterruptedException {
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.getDepDelay()> 0 ) { // 출발지연
			outputKey.setYear("D, "+parser.getYear());
			outputKey.setMonth(parser.getMonth());
			
			context.write(outputKey, one);
		}
		if(parser.getArrDelay()> 0 ) { // 도착지연
			outputKey.setYear("A, "+parser.getYear());
			outputKey.setMonth(parser.getMonth());
			
			context.write(outputKey, one);
		}
		
		super.map(key, value, context);
	}
	
}
