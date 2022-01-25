package myhadoop.mappers;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//MAPPER Ŭ������ ���
//�Է� : LineNo, �Ѷ���
//��� : �ܾ�, 1
public class WordCountMapper 
	extends Mapper<LongWritable, Text, // �Է� Ű Ÿ��, �Է� �� Ÿ�� 
					Text, IntWritable>{ //��� Ű Ÿ��, ��� �� Ÿ��

	private static final IntWritable one = new IntWritable(1);// one �� �̸� ������ ��
	//�ܾ ������ ����
	private Text word = new Text();
	
	//�� �޼��带 �����ϱ� ���� �������̵�
	@Override
	protected void map(LongWritable key, Text value, // �Է� Ű Ÿ��, �Է� �� Ÿ�� 
			Context context) // Map Reduce ���� ���� ����
			throws IOException, InterruptedException {
			//���� ������ ���� �۾� ����
		StringTokenizer st = new StringTokenizer(value.toString());
		//����
		while(st.hasMoreElements()) {
			word.set(st.nextToken());
			//(word, 1)
			context.write(word, one);
		}
	}
	
	
}
