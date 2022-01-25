package myhadoop.mappers;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//MAPPER 클래스를 상속
//입력 : LineNo, 한라인
//출력 : 단어, 1
public class WordCountMapper 
	extends Mapper<LongWritable, Text, // 입력 키 타입, 입력 값 타입 
					Text, IntWritable>{ //출력 키 타입, 출력 값 타입

	private static final IntWritable one = new IntWritable(1);// one 은 이름 지어준 것
	//단어를 저장할 변수
	private Text word = new Text();
	
	//맵 메서드를 구현하기 위해 오버라이드
	@Override
	protected void map(LongWritable key, Text value, // 입력 키 타입, 입력 값 타입 
			Context context) // Map Reduce 잡의 문맥 정보
			throws IOException, InterruptedException {
			//문장 분절을 위한 작업 시작
		StringTokenizer st = new StringTokenizer(value.toString());
		//루프
		while(st.hasMoreElements()) {
			word.set(st.nextToken());
			//(word, 1)
			context.write(word, one);
		}
	}
	
	
}
