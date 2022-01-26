package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;

//�Կ�Ű : ���ι�ȣ
//  �Է°� : csv
//	��� Ű : A, ��, �� or D, ��, ��
//	��°��� : 1
public class DelayCountMultimapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Ű
	private Text outputKey = new Text();
	//��°�
	private static final IntWritable one = new IntWritable(1);
	
	
	@Override
	protected void map(LongWritable key, 
			Text value, // csv 
			Context context)
			throws IOException, InterruptedException {
		//csv �м�
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		//������� -> DepDelay, �������� - ArrDelay
		if (parser.getDepDelay() > 0 ) { // �������
			//�츮�� ������ �� Ű�� ���°� D, ��, ��. �̰� ����ž�
			outputKey.set("D,"+parser.getYear()+", "+parser.getMonth()); // -> D, ��, ��
			//���
			context.write(outputKey, one);
		} else if(parser.getArrDelay() > 0 ) { //���������̶��
			outputKey.set("A,"+parser.getYear()+","+parser.getMonth()); // -> A, ��, ��
			//���
			context.write(outputKey, one);
		}
	}
	
	
}
