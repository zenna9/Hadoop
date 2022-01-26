package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DelayCountMultimapper;
import myhadoop.reducers.DelayCountMultiReducer;

public class DelayCountMulti {

	public static void main(String[] args) throws Exception{
		//����� ������ ���� Ȯ��
		if(args.length !=2) {
			System.err.println("Usage : DelayCountMulti <input><output>zzzennn");
			System.exit(2);
		}
		Configuration conf = new Configuration();// ���� �ҷ�����
		
		//Job����
		Job job = Job.getInstance(conf, "DelayCountMulti");
		
		//����� ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// ��� �ʰ� ���ེ Ŭ������ ���
		job.setJarByClass(DelayCountMulti.class);
		job.setMapperClass(DelayCountMultimapper.class);
		job.setReducerClass(DelayCountMultiReducer.class);
		
		//����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//��� Ű, �� Ÿ�� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//������� namdedOutput �����
		MultipleOutputs.addNamedOutput(job, 
				"departure", // �ƿ�ǲ �̸� 
				TextOutputFormat.class, // �ƿ�ǲ ��� ����
				Text.class, // �ƿ�ǲ ŰŸ��
				IntWritable.class); // �ƿ�ǲ ��Ÿ��
		MultipleOutputs.addNamedOutput(job, 
				"arrival", // 
				TextOutputFormat.class,
				Text.class,
				IntWritable.class);
				
		job.waitForCompletion(true);
		
		
		
	}

}
