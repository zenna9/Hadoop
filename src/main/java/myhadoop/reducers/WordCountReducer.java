package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 입력 : 매퍼로부터 전달된 출력
//		(단어, (1,1,1,1,1))
// 출력 : (단어, 단어카운트)
public class WordCountReducer 
		extends Reducer<Text, IntWritable, // 입력 키 타입, 입력 값 타입
		Text, IntWritable> { //출력 키 타입, 출력 값 타입

		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum =0 ;
			//집계 루프돌릴 것
			for (IntWritable value : values) {
				sum += value.get();
			}
			//결과설정
			result.set(sum);
			//최종적으로 출력
			context.write(key, result);
		}
		
		
}
