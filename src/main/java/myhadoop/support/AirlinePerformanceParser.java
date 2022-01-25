package myhadoop.support;

import org.apache.hadoop.io.Text;

//���������� csv�м��ϴ� Ŭ����
public class AirlinePerformanceParser {
	
	private int year;
	private int month;
	
	private String uniqueCarrier; // 

	private float arrDelay =0; // ���� ���� �ð�
	private float depDelay =0; // ��� ���� �ð�
	private float distance =0; // ���� �Ÿ�
	//private �ʵ�ϱ� �̾Ƴ� �� �ֵ��� getter �����
	// �������̵� �ϵ�, setters�������� getters�� ����
	
	//������
	public AirlinePerformanceParser(Text text) {
		//����ó��
		try { 
			//�޸��� �������� �߶���� ��-> split ��� ���
			String[] column = text.toString().split(",");
			//0�� �ε����� �÷��� ����־��
			year= Integer.parseInt(column[0]); // ���׿���
			month = Integer.parseInt(column[1]);// ���׿�
			uniqueCarrier = column[5];
			//�������� �ؽ�Ʈ�� ���� �ȹٲ��൵ ��
			
			//����ִ� null�÷��� �ִµ�, �̰� �ٲٵ��� ����� ��
			if (column[16].length() !=0) {
				depDelay = Float.parseFloat(column[16]); // ��� �����ð�
			}
			if (column[26].length() !=0) {
				arrDelay =Float.parseFloat(column[26]); // ��� �����ð�
			}
			if (column[37].length() !=0) {
				distance =Float.parseFloat(column[37]); // ��� �����ð�
			}
		} catch (Exception e) {
			System.err.println("Error Parsing : "+ e.getMessage());
		}
	}
	
	public float getArrDelay() {
		return arrDelay;
	}
	public void setArrDelay(float arrDelay) {
		this.arrDelay = arrDelay;
	}
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	public float getDepDelay() {
		return depDelay;
	}
	public float getDistance() {
		return distance;
	}
	
	
}
