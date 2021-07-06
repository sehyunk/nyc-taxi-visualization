#  Pydeck 뉴욕 텍시 데이터 시각화 연습

![image](/image.png)

## ref

- 변성윤님의 카일스쿨 학습자료를 참고했습니다. 
- https://github.com/zzsza/kyle-school



## Index

1. 빅쿼리에서 뉴욕 택시 데이터 전처리

- bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015
- bigquery-public-data.geo_us_boundaries.zip_codes (집코드 별 지오메트리 데이터)

2. python으로 불러와 시각화

- gbq를 이용해 python으로 데이터 불러오기

- pydeck을 통한 시각화

# 1. 빅쿼리에서 뉴욕 택시 데이터 전처리

- new_york_taxi_trips를 시각화 하기위해 데이터를 더 규칙성있게 만들어 줘야 함
- 각 텍시운영 데이터는 출발지의 위경도, 도착지의 위경도를 가지고 있는데,
- 실제 우리가 내리는 곳이 제각각이듯 위경도의 값이 '40.776935577392578' 이런식으로 너무나 다양함
  - '잠실역에서 성수역이요~'하듯이 좀 더 균일화된 위경도 값으로 변경작업이 필요
  - bigquery-public-data.geo_us_boundaries.zip_codes 데이터 테이블에는 zipcode별 위경도 데이터가 포함되어 있는데, 해당 테이블과 'st_contains, st_geogpoint'이 두가지 함수로 위경도를 균일화 할 수 있음
    - st_contains(geo1, geo2): geo1의 위경도 값들 안에 geo2가 포함되면 True 아니면 False
    - st_geogpoint(lon, lat): 위경도 값을  GEOGRAPHY 형태로 반환(st_contains함수에 넣기위함)
    - 함수에 대한 상세설명은 [여기를 참고](https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions?hl=ko#st_contains)
- zipcode별 위경도로 치환한 데이터를 시간대별, 위경도별로 group by

```sql
# 위경도를 간략화 하기위한 db 테이블
with db as (
    select taxi.*,
  					# taxi의 실제 위경도 데이터를 zipcode에 해당하는 위경도 데이터로 바꿔서 사용합니다.
 						# internal_point_lat은 특정 zipcode의 위도입니다.
            pick.internal_point_lat as pickup_zip_code_lat,
            pick.internal_point_lon as pickup_zip_code_lon,
            drop.internal_point_lat as dropoff_zip_code_lat,
            drop.internal_point_lon as dropoff_zip_code_lon
    from (
        select *
        from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015`
      	# 1월 1일의 데이터만 필터링 합니다.
				where extract (date from pickup_datetime) = '2015-01-01'
      			# 위도는 -90에서 90까지만 유효합니다. 그 외의 데이터는 오류이므로 제외합니다.
            and pickup_latitude between -90 and 90
            and dropoff_latitude between -90 and 90
    ) as taxi # 실제 운영 기록이 담긴 taxi테이블
        join (
            select * 
            from `bigquery-public-data.geo_us_boundaries.zip_codes`
          	# 뉴욕시의 데이터만 필터링, 뉴욕 외의 지역에서 픽업한 데이터는 제외됩니다.
            where state_code = 'NY'
        ) as pick # 실제 픽업 위치의 위경도를 zipcode의 대표 위경도로 단일화시킬 pick테이블(join후 테이블 치환)
  					# taxi테이블의 위경도가 어느 zipcode에 해당되는지 찾아서 join시키기 위한 부분
            on st_contains(pick.zip_code_geom, st_geogpoint(pickup_longitude, pickup_latitude))
        join (
            select *
            from `bigquery-public-data.geo_us_boundaries.zip_codes`
            where state_code = 'NY'
        ) as drop # 실제 드랍 위치의 위경도를 치환할 drop테이블
            on st_contains(drop.zip_code_geom, st_geogpoint(dropoff_longitude, dropoff_latitude))
)
select datetime_trunc(pickup_datetime, hour) as pickup_hour, # 시간대별로 집계
    # 픽업/드랍 위경도: zipcode별로 단일화되어서 집계가 가능해짐
    pickup_zip_code_lat,
    pickup_zip_code_lon,
    dropoff_zip_code_lat,
    dropoff_zip_code_lon,
    count(*) as cnt
from db
# 시간, 픽업/드랍 위경도별로 그룹바이
group by 1,2,3,4,5
# 집계 후 가운트가 적은 경우는 필터링
having cnt >= 20

```

# 2. python으로 불러와 시각화

- [nbviewer에서 코드 보기](https://nbviewer.jupyter.org/github/sehyunk/nyc-taxi-visualization/blob/master/nyc-taxi-visualization.ipynb)
- pydeck 결과물은 노트북에서 보이지 않아 html로 첨부
