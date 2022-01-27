package myhadoop.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// 복합키 사용
public class DateKey implements WritableComparable<DateKey>{
	private String year;
	private Integer month;
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out,year);
		out.writeInt(month);
	}
	
	
	// 스트림에서 필드 데이터를 읽어들이는 메서드
	// 스트림에서 데이터 읽을 때는 WritableUtils를 이용
	@Override
	public void readFields(DataInput in) throws IOException {
		// 입력 스트림에서 데이터 조회
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	@Override
	public int compareTo(DateKey other) {
		//year 필드로 순번 정하기
		int result = year.compareTo(other.year);
		
		if(result ==0 ) { // 두 복합 키의 년도가 동일 순번
			result = month.compareTo(other.month); // 두 복합 키의 월 정보의 순번 비교
			
		}
		return result;
	}
	public DateKey() {
		
	}
	public DateKey(String year, Integer month) {
		this.year = year;
		this.month	= month;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}


	@Override
	public String toString() {
		return year+ "," + month ;
	}
	
	
}
