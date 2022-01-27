package myhadoop.support;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupKeyComparator extends WritableComparator{
	protected GroupKeyComparator() {
		super(DateKey.class, true);
		//두개의 키를 비교해줘야하니까 오버라이드
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey)b;
		
		//연도비교
		return k1.getYear().compareTo(k2.getYear());
	}

}
