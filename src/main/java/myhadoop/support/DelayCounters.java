package myhadoop.support;

// 사용자 정의 카운터를 위한 enum
// 상수들의 묶음
public enum DelayCounters {
	scheduled_arrival, //정시도착 카운터
	early_arrival, //일찍 도착한 카운터 
	scheduled_departure, // 정시출발 카운터
	early_departure, //일찍 출발한 카운터

}
