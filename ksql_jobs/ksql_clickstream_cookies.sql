create stream clickstream_eid_time (
	rowkey varchar key,
	eid int,
	uid int,
	_server_time bigint,
	_ua_is_test int
) WITH (KAFKA_TOPIC='CLICKSTREAM_CORE',VALUE_FORMAT='JSON',TIMESTAMP='_server_time');


create table clickstream_cookies  as
select 
	rowkey,
	windowstart as agg_time,
	latest_by_offset(rowkey) as u,
	max(case when uid > 0 then 1 else 0 end) as is_logged_in,
	sum(case when eid = 3506 then 1 else 0 end) as cnt_pixel,
	sum(case when eid = 301 then 1 else 0 end) as cnt_item_view,
	sum(case when eid = 303 then 1 else 0 end) as cnt_phone_view,
	sum(case when eid = 2649 then 1 else 0 end) as cnt_app_item,
	sum(case when eid = 2650 then 1 else 0 end) as cnt_app_call
from clickstream_eid_time 
WINDOW HOPPING(SIZE 10 minutes, ADVANCE BY 2 minute, RETENTION 15 minutes, GRACE PERIOD 1 minutes)
where rowkey is not null and rowkey > '' 
  and _ua_is_test = 0
  and (eid = 3506 or eid = 301 or eid = 303 or eid = 2649 or eid = 2650)
group by rowkey
EMIT CHANGES;
--EMIT FINAL;

