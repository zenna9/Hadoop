package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.AirlinePerformanceParser;
import myhadoop.support.DelayCounters;

public class DelayCountMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String workType;
		private final static IntWritable one = new IntWritable(1);
		private Text ouputkey = new Text();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			workType = context.getConfiguration().get("workType"); // ����� ���� �ɼ� �б�
		}
		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			AirlinePerformanceParser parser = new AirlinePerformanceParser(value); 
			
			if (workType.equals("departure")) {
				if(parser.getDepDelay()>0) {
					ouputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(ouputkey , one);
				}
				
				//�ڡڡڡڡ����� ���, ���� ��� ������ ����͸� -> Counter
				else if (parser.getDepDelay()==0) {
					context.getCounter(DelayCounters.scheduled_departure).increment(1);
				} else if (parser.getDepDelay() <0 ) { // ���� ���
					context.getCounter(DelayCounters.early_departure).increment(1);
					
				}
				//�ڡڡڡڡ� ������� ����
				
			} else if ( workType.equals("arrival")) { // ���� ���� ī��Ʈ ����
				if(parser.getArrDelay() >0) {
					ouputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(ouputkey , one);
					//�ڡڡڡڡ�	
			}else if(parser.getArrDelay() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if(parser.getArrDelay() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		}
		}}
		
	

