package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;

//입열키 : 라인번호
//  입력값 : csv
//	출력 키 : A, 년, 월 or D, 년, 월
//	출력값은 : 1
public class DelayCountMultimapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//키
	private Text outputKey = new Text();
	//출력값
	private static final IntWritable one = new IntWritable(1);
	
	
	@Override
	protected void map(LongWritable key, 
			Text value, // csv 
			Context context)
			throws IOException, InterruptedException {
		//csv 분석
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		//출발지연 -> DepDelay, 도착지연 - ArrDelay
		if (parser.getDepDelay() > 0 ) { // 출발지연
			//우리가 만들기로 한 키의 형태가 D, 년, 월. 이걸 만들거야
			outputKey.set("D,"+parser.getYear()+", "+parser.getMonth()); // -> D, 년, 월
			//출력
			context.write(outputKey, one);
		} else if(parser.getArrDelay() > 0 ) { //도착지연이라면
			outputKey.set("A,"+parser.getYear()+","+parser.getMonth()); // -> A, 년, 월
			//출력
			context.write(outputKey, one);
		}
	}
	
	
}
