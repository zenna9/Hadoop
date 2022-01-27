package myhadoop.support;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// �ΰ� ����Ű�� ���ϴ� Ŭ����
public class DateKeyComparator extends WritableComparator{
	// ������ 
	protected DateKeyComparator() {
		super(DateKey.class, true);
		
	}

	// �ΰ��� WritableComparable ��ü�� ��
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//ĳ������ �ʿ�
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey)b;
		//k1�� k2��
		//�⵵��
		int cmp = k1.getYear().compareTo(k2.getYear()); // 2012�� 2013 ���� �� ����� �ƴϴϱ�..
		if (cmp != 0 ) { // ������ �����ʴٸ� -> ���� ����� �ƴ�
			return cmp;
		}
		//�⵵�� ������ �ι�° ������ �������� ��
		
		return k1.getMonth().compareTo(k2.getMonth());
		
	}

}
