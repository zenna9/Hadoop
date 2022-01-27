package myhadoop.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// ����Ű ���
public class DateKey implements WritableComparable<DateKey>{
	private String year;
	private Integer month;
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out,year);
		out.writeInt(month);
	}
	
	
	// ��Ʈ������ �ʵ� �����͸� �о���̴� �޼���
	// ��Ʈ������ ������ ���� ���� WritableUtils�� �̿�
	@Override
	public void readFields(DataInput in) throws IOException {
		// �Է� ��Ʈ������ ������ ��ȸ
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	@Override
	public int compareTo(DateKey other) {
		//year �ʵ�� ���� ���ϱ�
		int result = year.compareTo(other.year);
		
		if(result ==0 ) { // �� ���� Ű�� �⵵�� ���� ����
			result = month.compareTo(other.month); // �� ���� Ű�� �� ������ ���� ��
			
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
