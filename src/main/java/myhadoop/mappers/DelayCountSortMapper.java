package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;
import myhadoop.support.DateKey;

//�Է� Ű : ���γѹ�, 	�� : csv
// ���Ű : DateKey, 	�� : 1
public class DelayCountSortMapper extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	// �� ��� ��
	private static final IntWritable one= new IntWritable(1);
	private DateKey outputKey = new DateKey();
	
	
	@Override
	protected void map(LongWritable key, 
			Text value,  // csv
			Context context)
			throws IOException, InterruptedException {
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.getDepDelay()> 0 ) { // �������
			outputKey.setYear("D, "+parser.getYear());
			outputKey.setMonth(parser.getMonth());
			
			context.write(outputKey, one);
		}
		if(parser.getArrDelay()> 0 ) { // ��������
			outputKey.setYear("A, "+parser.getYear());
			outputKey.setMonth(parser.getMonth());
			
			context.write(outputKey, one);
		}
		
		super.map(key, value, context);
	}
	
}
