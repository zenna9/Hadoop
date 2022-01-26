package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.WordCountMapper;
import myhadoop.reducers.WordCountReducer;
//import myhadoop.reducers.WordCountReducer;

public class WordCount {
//		1. �� ��ü ����
//		2. �ʸ��ེ �⿡ �ʿ��� ���� ������ ����
//		3. �ʸ��ེ ���� ����
//		WordCount <����> <output>

	public static void main(String[] args) throws Exception {
		// �Ű����� üũ
		if (args.length !=2) {
			System.err.println("Usage:WordCount <input> <output>");
			System.exit(2);
			
		}
		
		Configuration conf = new Configuration(); // ���� �ҷ�����
		
		Job job = Job.getInstance(conf, "WordCount"); 
		// ���ο��� ó���� ����� �� �ٱ����� �������� throws�� ���� (���� �߻��� �����ع������)
		job.setJarByClass(WordCount.class);// ����̹� Ŭ���� ���
		job.setMapperClass(WordCountMapper.class);//���� ���
		job.setReducerClass(WordCountReducer.class); // ���༭ ���

		// ����� ������ ���� ���
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//���Ű, ���� Ÿ�� ���
		// (�ܾ�, ���谪)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//����� ��ġ ���� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//�ϵ� ��ü���� �� ����
		job.waitForCompletion(true);
		
	}

}
