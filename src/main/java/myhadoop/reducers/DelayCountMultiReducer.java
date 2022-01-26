package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// ���� ����� ���ؼ� ���༭�� MultipleOutputs�� ����
//	Ű: D, ��, ��		
//	�Է°� : (1, 1, 1, 1...)
// 	���Ű : ��, ��
// 	��°� : ������
public class DelayCountMultiReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	// ������� ����
	private MultipleOutputs<Text, IntWritable> mos;
	// reduce ���Ű
	private Text outputKey = new Text();
	// reduce ��°�
	private IntWritable outputValue = new IntWritable();

	//setup �޼��尡 ����ż� �ʱ�ȭ. -> reduce�޼��� ���� -> cleanup �޼��� ����
	//setup �޼���
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//���༭ �ʱ�ȭ
		mos = new MultipleOutputs<Text, IntWritable>(context);
				
				
	}
	// 	Ű : ������� -> D, ��, �� (: �̰� �Ϸ��� ���ڿ��� Ư�� �����ڷ� �ɰ���� ��)
	//		�������� -> A, ��, ��
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String columns[] = key.toString().split(","); // , �� �������� �и��ϵ��� ��
		// �� Ű�� ����
		outputKey.set(columns[1]+","+columns[2]); // -> ��, ��
		
		//����
		int sum =0 ; 
		for(IntWritable value:values) {
			sum += value.get();
		}
		outputValue.set(sum);
		//��¹��� �б�
		// ���ξ D�� ��������� ���õ� ���̹Ƿ� 
		if (columns[0].equals("D")) { //������� ����
			// ��� ���� ����
			mos.write("departure", outputKey, outputValue);
		}else if (columns[0].equals("A")) { // ���� ���� ����
			mos.write("arrival", outputKey, outputValue);
		}
	}

	//�ڿ�����
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
		
	}
	

}
