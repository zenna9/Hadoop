package myhadoop.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import myhadoop.support.DateKey;

//�Է�Ű : DateKey, 	�Է°� : (1, 1, 1, ...)
// ���Ű : Datekey, ��°� : ���� ���
public class DelayCountSortReducer extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{

}
