package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import myhadoop.support.DateKey;

//입력키 : DateKey, 	입력값 : (1, 1, 1, ...)
// 출력키 : Datekey, 출력값 : 집계 결과
public class DelayCountSortReducer extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{
 //출력 경로 만들기
	private MultipleOutputs<DateKey, IntWritable> mos;
	
	private DateKey outputKey = new DateKey();
	private IntWritable outputValue = new IntWritable();
	
	
	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
//		출발지연("D,년", 월) -> departure출력
//		도착지연("A,년", 월) -> arrival 출력
		String columns[] = key.getYear().split(","); // {"D","년"} 모양으로 나오게 될 것
		
		//집계값
		int sum = 0;
		Integer month = key.getMonth(); // 월 
		
		if(columns[0].equals("D")) { // -> departure아웃풋으로 출력
			for (IntWritable value: values) {
				if(month != key.getMonth()) {
					// 월 정보가 변경되었다면 중간에라도 정보를 내보내라
					outputValue.set(sum); // 합산 결과 설정
					outputKey.setYear(columns[1]);
					outputKey.setMonth(month);
					
					// 다중출력으로 내보내기
					mos.write("departure", outputKey, outputValue);
					sum = 0; // 집계 초기화
				}
				sum +=value.get();
				month = key.getMonth();
			}
			if(key.getMonth() == month) {
				outputKey.setYear(columns[1]);
				outputKey.setMonth(key.getMonth());
				outputValue.set(sum);
				
				// 출력
				mos.write("departure", outputKey, outputValue);
			}
		}
		if(columns[0].equals("A")) { // arrival 출력으로 ㄴ..
			for (IntWritable value: values) {
				if(month != key.getMonth()) {
					// 월 정보가 변경되었다면 중간에라도 정보를 내보내라
					outputValue.set(sum); // 합산 결과 설정
					outputKey.setYear(columns[1]);
//					outputKey.setMonth(key.getMonth());
					// 다중출력으로 내보내기
					outputKey.setMonth(month);
					mos.write("arrival", outputKey, outputValue);
					sum = 0; // 집계 초기화
				}
				sum +=value.get();
				month = key.getMonth();
			}
			if(key.getMonth() == month) {
				outputKey.setYear(columns[1]);
				outputKey.setMonth(key.getMonth());
				outputValue.set(sum);
				mos.write("arrival", outputKey, outputValue);
				
			}
		}
	}

	//리듀서 초기화 작업 할 것
	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}

	@Override
	protected void cleanup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 정리 작업
		mos.close();
		}
}
