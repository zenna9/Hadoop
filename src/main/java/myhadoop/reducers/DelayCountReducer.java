package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//입력키 : "년, 월"       입력값:(1, 1, 1, 1....)
//	  출력키 : "년, 월"       출력값 : 집계
public class DelayCountReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable result = new IntWritable();

	//오버라이드
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum =0 ;
		for (IntWritable value : values) {
			sum += value.get();
			
		}
		
		//출력
		result.set(sum);
		context.write(key, result);
	}
	
	
	
	
}
