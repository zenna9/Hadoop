package myhadoop.support;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<DateKey,// �� �׽�Ʈ�� ��� Ű Ÿ�� 
								IntWritable>{ // �� �׽�ũ�� ��� �� Ÿ��

	@Override
	public int getPartition(DateKey key, 	// ŰŸ�� 
			IntWritable value, 				// ���� Ÿ�� 
			int numPartitions) {			// ��Ƽ���� ����
		int hash = key.getClass().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	} 
	

}
