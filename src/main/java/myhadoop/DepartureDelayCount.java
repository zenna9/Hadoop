package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import myhadoop.mappers.DepartureDelayCountMapper;
import myhadoop.reducers.DelayCountReducer;

public class DepartureDelayCount {

	public static void main(String[] args) throws Exception{
		//����� ������ ��� Ȯ��
		if (args.length !=2) {
			System.err.println("Usage : DepartureDelayCount <input> <output>");
			System.exit(2);
		}
		
		//�ϵ� ���� �ε�
		Configuration conf = new Configuration();
		
		//job �̸� ����
		Job job = Job.getInstance(conf, "DepartureDelayCount");
		
		//����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// ��, ����, ���༭ ���
		job.setJarByClass(DepartureDelayCount.class);
		job.setMapperClass(DepartureDelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		//����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//��� Ű, �� ���� ����
		job.setOutputKeyClass(Text.class);// ��,��
		job.setOutputValueClass(IntWritable.class);
		
		//Job ����
		job.waitForCompletion(true);
		
	}

}
