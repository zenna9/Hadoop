package myhadoop.support;

import org.apache.hadoop.io.Text;

//원본데이터 csv분석하는 클래스
public class AirlinePerformanceParser {
	
	private int year;
	private int month;
	
	private String uniqueCarrier; // 

	private float arrDelay =0; // 도착 지연 시간
	private float depDelay =0; // 출발 지연 시간
	private float distance =0; // 운항 거리
	//private 필드니까 뽑아낼 수 있도록 getter 만들기
	// 오버라이드 하되, setters하지말고 getters만 선택
	
	//생성자
	public AirlinePerformanceParser(Text text) {
		//예외처리
		try { 
			//콤마를 기준으로 잘라줘야 함-> split 기능 사용
			String[] column = text.toString().split(",");
			//0번 인덱스의 컬럼을 집어넣어보재
			year= Integer.parseInt(column[0]); // 운항연도
			month = Integer.parseInt(column[1]);// 운항월
			uniqueCarrier = column[5];
			//나머지는 텍스트라 굳이 안바꿔줘도 됨
			
			//비어있는 null컬럼이 있는데, 이걸 바꾸도록 해줘야 함
			if (column[16].length() !=0) {
				depDelay = Float.parseFloat(column[16]); // 출발 지연시간
			}
			if (column[26].length() !=0) {
				arrDelay =Float.parseFloat(column[26]); // 출발 지연시간
			}
			if (column[37].length() !=0) {
				distance =Float.parseFloat(column[37]); // 출발 지연시간
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
