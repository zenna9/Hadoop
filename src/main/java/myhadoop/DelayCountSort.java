package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DelayCountSortMapper;
import myhadoop.reducers.DelayCountSortReducer;
import myhadoop.support.DateKey;
import myhadoop.support.DateKeyComparator;
import myhadoop.support.GroupKeyComparator;
import myhadoop.support.GroupKeyPartitioner;

public class DelayCountSort {

	public static void main(String[] args) throws Exception {
		//����� ������ ��� Ȯ��
		if (args.length != 2) {
			System.err.println("Usage : DelayCountSort <input><output>");
			System.exit(2);
			
		}
		
		//���� �ҷ�����
		Configuration conf = new Configuration();
		
		//�� ����, ���⼭ �������� ����ϱ� ���� ������ throws�� �������ִ°���
		Job job = Job.getInstance(conf, "DelayCountSort");
		
		//����� ���
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��, ����, ���༭ ���
		job.setJarByClass(DelayCountSort.class);
		//��Ƽ�ų� Ŭ���� ���
		job.setPartitionerClass(GroupKeyPartitioner.class);
		//�׷� Ű �񱳱� ���
		job.setCombinerKeyGroupingComparatorClass(GroupKeyComparator.class);
		//������ ���� ����Ű �񱳱� ���
		job.setSortComparatorClass(DateKeyComparator.class);
		
		job.setMapperClass(DelayCountSortMapper.class);
		job.setReducerClass(DelayCountSortReducer.class);
		
		//���� ��� Ű, ���� Ÿ�� ����
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//���༭ ���Ű�� ���� Ÿ��
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		//����� ������ ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//���� ��� ���� : departure, arrival
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);
		
		job.waitForCompletion(true);
		
		
	}

}
