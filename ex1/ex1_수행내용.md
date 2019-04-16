# ex1 수행 내용입니다.

//1. 로컬에 있는 devicestatus.txt를 hdfs로 복사

//2. hdfs의 ex1 디렉토리 내 devicestatus.txt 읽음
- devicestatusfile = sc.textFile("/ex1/devicestatus.txt")

//3. devicestatus.txt 내 내용을 ',' 단위로 split 처리하여 map 형태로 변환
- statusdata = devicestatusfile.map( lambda line: line.split(',') );

//4. row별 array갯수가 14개가 아닌( 정상데이터가 아닌 ) row 필터 처리
- filterd_data2 = statusdata.filter( lambda arr: len(arr) == 14 );

//5. date, manufacturer, model, device ID, latitude, longitude 항목 추출
- final_data = filterd_data2.map( lambda data: ( data[0], "manufacturer : " + data[1].split(' ')[0], "model : " + data[1].replace(data[1].split(' ')[0] , "") ,data[2], data[12], data[13] ) )

//6. 최종 데이터 저장
- final_data.saveAsTextFile("/loudacre/devicestatus_etl");
