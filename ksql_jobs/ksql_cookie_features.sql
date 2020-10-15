
create stream clickstream_cookies 
(
	u varchar,
	agg_time bigint,
	cnt_pixel int,
	cnt_item_view int,
	cnt_phone_view int,
	cnt_app_item int,
	cnt_app_call int
) WITH (KAFKA_TOPIC='CLICKSTREAM_COOKIES',VALUE_FORMAT='JSON');


create table cookie_features as 
select 
	u,
	max(agg_time) as agg_time,
	max(cnt_pixel) > 0 as has_pixel,
	max(case when cnt_item_view >= 5 then coalesce(cnt_phone_view,0) / cnt_item_view else 0 end) > 0.95 as has_ctr_broken,
	min(case when cnt_item_view >= 5 then coalesce(cnt_app_item,0) / cnt_item_view else 1 end) < 0.2 as has_frontback_iv_broken,
	min(case when cnt_phone_view >= 5 then coalesce(cnt_app_call,0) / cnt_phone_view else 1 end) < 0.2 as has_frontback_ph_broken
from clickstream_cookies 
group by u
emit changes;
--emit final;
