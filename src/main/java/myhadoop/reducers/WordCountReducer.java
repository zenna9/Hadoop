package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// �Է� : ���۷κ��� ���޵� ���
//		(�ܾ�, (1,1,1,1,1))
// ��� : (�ܾ�, �ܾ�ī��Ʈ)
public class WordCountReducer 
		extends Reducer<Text, IntWritable, // �Է� Ű Ÿ��, �Է� �� Ÿ��
		Text, IntWritable> { //��� Ű Ÿ��, ��� �� Ÿ��

		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum =0 ;
			//���� �������� ��
			for (IntWritable value : values) {
				sum += value.get();
			}
			//�������
			result.set(sum);
			//���������� ���
			context.write(key, result);
		}
		
		
}
