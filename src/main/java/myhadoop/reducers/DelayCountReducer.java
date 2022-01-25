package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//�Է�Ű : "��, ��"       �Է°�:(1, 1, 1, 1....)
//	  ���Ű : "��, ��"       ��°� : ����
public class DelayCountReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable result = new IntWritable();

	//�������̵�
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum =0 ;
		for (IntWritable value : values) {
			sum += value.get();
			
		}
		
		//���
		result.set(sum);
		context.write(key, result);
	}
	
	
	
	
}
