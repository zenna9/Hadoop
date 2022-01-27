package myhadoop.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import myhadoop.support.DateKey;

//입력키 : DateKey, 	입력값 : (1, 1, 1, ...)
// 출력키 : Datekey, 출력값 : 집계 결과
public class DelayCountSortReducer extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{

}
