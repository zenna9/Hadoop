package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// 다중 출력을 위해서 리듀서가 MultipleOutputs를 정의
//	키: D, 년, 월		
//	입력값 : (1, 1, 1, 1...)
// 	출력키 : 년, 월
// 	출력값 : 집계결과
public class DelayCountMultiReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	// 다중출력 선언
	private MultipleOutputs<Text, IntWritable> mos;
	// reduce 출력키
	private Text outputKey = new Text();
	// reduce 출력값
	private IntWritable outputValue = new IntWritable();

	//setup 메서드가 수행돼서 초기화. -> reduce메서드 수행 -> cleanup 메서드 수행
	//setup 메서드
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//리듀서 초기화
		mos = new MultipleOutputs<Text, IntWritable>(context);
				
				
	}
	// 	키 : 출발지연 -> D, 년, 월 (: 이걸 하려면 문자열을 특정 구분자로 쪼개줘야 함)
	//		도착지연 -> A, 년, 월
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String columns[] = key.toString().split(","); // , 를 기준으로 분리하도록 함
		// 새 키를 생성
		outputKey.set(columns[1]+","+columns[2]); // -> 년, 월
		
		//집계
		int sum =0 ; 
		for(IntWritable value:values) {
			sum += value.get();
		}
		outputValue.set(sum);
		//출력방향 분기
		// 접두어가 D면 출발지연에 관련된 것이므로 
		if (columns[0].equals("D")) { //출발지연 집계
			// 출력 방향 결정
			mos.write("departure", outputKey, outputValue);
		}else if (columns[0].equals("A")) { // 도착 지연 집계
			mos.write("arrival", outputKey, outputValue);
		}
	}

	//자원정리
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
		
	}
	

}
