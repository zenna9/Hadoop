package myhadoop.support;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<DateKey,// 맵 테스트의 출력 키 타입 
								IntWritable>{ // 맵 테스크의 출력 값 타입

	@Override
	public int getPartition(DateKey key, 	// 키타입 
			IntWritable value, 				// 값의 타입 
			int numPartitions) {			// 파티션의 개수
		int hash = key.getClass().hashCode();
		int partition = hash % numPartitions;
		
		return partition;
	} 
	

}
