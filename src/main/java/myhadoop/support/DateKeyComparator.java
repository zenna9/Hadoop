package myhadoop.support;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 두개 복합키를 비교하는 클래스
public class DateKeyComparator extends WritableComparator{
	// 생성자 
	protected DateKeyComparator() {
		super(DateKey.class, true);
		
	}

	// 두개의 WritableComparable 객체를 비교
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//캐스팅이 필요
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey)b;
		//k1과 k2비교
		//년도비교
		int cmp = k1.getYear().compareTo(k2.getYear()); // 2012와 2013 년은 비교 대상이 아니니까..
		if (cmp != 0 ) { // 순번이 같지않다면 -> 솔팅 대상이 아님
			return cmp;
		}
		//년도가 같으면 두번째 기준인 월정보로 비교
		
		return k1.getMonth().compareTo(k2.getMonth());
		
	}

}
