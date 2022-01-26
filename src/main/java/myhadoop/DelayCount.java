package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import myhadoop.mappers.DelayCountMapper;
import myhadoop.reducers.DelayCountReducer;

// ȯ�溯�� ��� ���� ConfiguredŬ���� ��ӹ�
	//����� ���� �ɼ� ��� ���� Tool �������̽� ����
public class DelayCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new DelayCount(), args);
		System.out.println("MapReduce-Job Result :"+ result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// ���޹��� args ���� �Ű����� �м�
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		//- -conf, -fs, -D ���� �ϵ��� ���� �ɼ��� ������ ������ ���ڸ� �� �� ����
		//����� ��� Ȯ��
		if (otherArgs.length != 2) {
			System.err.println("Usage: DelayCount -DworkType=departure | arrival <input><output>");
			System.exit(2);
		}
		//Job��������� ��
		Job job = Job.getInstance(getConf(),"DelayCount");
		//����� ����� ����. ��ǲ�� �ƿ�ǲ�� otherArgs�� ������
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//����̹�Ŭ������ ����Ŭ����, ���༭Ŭ���� ���
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		//����� ������ ���� ����. �� ������ �̾���
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//��� Ű, �� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//����̹� Ŭ���� ����
		job.waitForCompletion(true);
		
		return 1;
	}



}
