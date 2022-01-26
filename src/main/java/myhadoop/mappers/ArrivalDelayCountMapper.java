package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;

// �Է� : Ű - ���ι�ȣ,       �� - csv (��, ��, ��, ��,,,,)
// ��� : Ű - �⵵,��         �� - 1
public class ArrivalDelayCountMapper 
		extends Mapper<LongWritable, Text, // �Է� ŰŸ��, �Է� ��Ÿ�� 
					Text, IntWritable >{ // ��� ŰŸ��, ��� ��Ÿ��
	private final static IntWritable one = new IntWritable(1); // ������ ��°� ==1
	private Text outputKey = new Text();
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	//�������� ù��° ���� �����Ͱ� �ƴ� header�� ��ŵ�ϵ��� 
		//key���� 0�̰� "YEAR"�̶�� �ؽ�Ʈ�� ���Ե����� ����ϰŷ�
		if (key.get() == 0 && value.toString().contains("YEAR")) {
			return;
		}
		//csv �м�
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		//��� Ű ����
		outputKey.set(parser.getYear()+","+parser.getMonth());
		//��� �����ð�(depDelay) >0�̸� ���
		if (parser.getArrDelay() >0) {
			//���
			context.write(outputKey, one); 
		}
	} 
	
	
}
