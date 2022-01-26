package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;

// 입력 : 키 - 라인번호,       값 - csv (값, 값, 값, 값,,,,)
// 출력 : 키 - 년도,월         값 - 1
public class ArrivalDelayCountMapper 
		extends Mapper<LongWritable, Text, // 입력 키타입, 입력 값타입 
					Text, IntWritable >{ // 출력 키타입, 출력 값타입
	private final static IntWritable one = new IntWritable(1); // 매퍼의 출력값 ==1
	private Text outputKey = new Text();
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	//데이터의 첫번째 행이 데이터가 아닌 header면 스킵하도록 
		//key값이 0이고 "YEAR"이라는 텍스트가 포함돼있음 헤더일거래
		if (key.get() == 0 && value.toString().contains("YEAR")) {
			return;
		}
		//csv 분석
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		//출력 키 설정
		outputKey.set(parser.getYear()+","+parser.getMonth());
		//출발 지연시간(depDelay) >0이면 출력
		if (parser.getArrDelay() >0) {
			//출력
			context.write(outputKey, one); 
		}
	} 
	
	
}
