package myhadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import myhadoop.support.DateKey;

//�Է�Ű : DateKey, 	�Է°� : (1, 1, 1, ...)
// ���Ű : Datekey, ��°� : ���� ���
public class DelayCountSortReducer extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{
 //��� ��� �����
	private MultipleOutputs<DateKey, IntWritable> mos;
	
	private DateKey outputKey = new DateKey();
	private IntWritable outputValue = new IntWritable();
	
	
	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
//		�������("D,��", ��) -> departure���
//		��������("A,��", ��) -> arrival ���
		String columns[] = key.getYear().split(","); // {"D","��"} ������� ������ �� ��
		
		//���谪
		int sum = 0;
		Integer month = key.getMonth(); // �� 
		
		if(columns[0].equals("D")) { // -> departure�ƿ�ǲ���� ���
			for (IntWritable value: values) {
				if(month != key.getMonth()) {
					// �� ������ ����Ǿ��ٸ� �߰����� ������ ��������
					outputValue.set(sum); // �ջ� ��� ����
					outputKey.setYear(columns[1]);
					outputKey.setMonth(month);
					
					// ����������� ��������
					mos.write("departure", outputKey, outputValue);
					sum = 0; // ���� �ʱ�ȭ
				}
				sum +=value.get();
				month = key.getMonth();
			}
			if(key.getMonth() == month) {
				outputKey.setYear(columns[1]);
				outputKey.setMonth(key.getMonth());
				outputValue.set(sum);
				
				// ���
				mos.write("departure", outputKey, outputValue);
			}
		}
		if(columns[0].equals("A")) { // arrival ������� ��..
			for (IntWritable value: values) {
				if(month != key.getMonth()) {
					// �� ������ ����Ǿ��ٸ� �߰����� ������ ��������
					outputValue.set(sum); // �ջ� ��� ����
					outputKey.setYear(columns[1]);
//					outputKey.setMonth(key.getMonth());
					// ����������� ��������
					outputKey.setMonth(month);
					mos.write("arrival", outputKey, outputValue);
					sum = 0; // ���� �ʱ�ȭ
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

	//���༭ �ʱ�ȭ �۾� �� ��
	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}

	@Override
	protected void cleanup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// ���� �۾�
		mos.close();
		}
}
