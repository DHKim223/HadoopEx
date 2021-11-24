package airplane;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// 월별 지연 데이터 처리			//2008년 데이터
public class DelayParser {
	private int year; 
	private int month;
	private int arriveDelay = 0; 										// 도착지연시간 (분)
	private int departureDelay = 0; 								//출발지연시간 (분)
	private int distance =0; 												//거리 (mile)
	private boolean arriveDelayAvailable = true;			//도착지연
	private boolean departureDelayAvailable = true; 	// 출발지연
	private String uniqueCarrier; 									//항공사 코ㅗ드
	private boolean distanceAvailable = true;				// 운향여부
	
	
	public DelayParser(LongWritable key, Text value) {
		if (key.get() == 0) {		//첫줄 제목삭제
			return;
		}
		String [] columns = value.toString().split(",");
		year = Integer.parseInt(columns[0]);						//년도
		month = Integer.parseInt(columns[1]);					//월
		uniqueCarrier = columns[8];									//항공사 코드
		if(!columns[14].equals("NA")) {								//도착지연 했다
			departureDelay = Integer.parseInt(columns[14]);
		} else {
			departureDelayAvailable = false;						// =도착지연을 안했다
		}
		
		if (! columns[15].equals("NA")) {
			departureDelay = Integer.parseInt(columns[15]); 	// 출발지연 했다
		} else {
			departureDelayAvailable = false;							//출발지연 안했다
		}
		
		if(! columns[18].equals("NA")) {									//운항 했다
			distance = Integer.parseInt(columns[18]);
			
		} else {
			distanceAvailable = false;									//운항 안했다
		}
		
		
		
	} // 생성자


	public int getYear() {
		return year;
	}


	public int getMonth() {
		return month;
	}


	public int getArriveDelay() {
		return arriveDelay;
	}


	public int getDepartureDelay() {
		return departureDelay;
	}


	public int getDistance() {
		return distance;
	}


	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	
	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}
	
	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}
	
	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}
	
}
