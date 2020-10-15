create stream clickstream_flat (
	rowkey varchar key,
	uuid varchar,
	dt int,
	dtm double,
	eid int,
	src_id int,
	src int,
	app int, 
	uid int,
	ua varchar,
	cid int,
	params varchar,
	infm_raw_params varchar,
	`$` varchar
) WITH (KAFKA_TOPIC='user-keyed-clickstream',VALUE_FORMAT='JSON');


create stream clickstream_core with(KAFKA_TOPIC='CLICKSTREAM_CORE',VALUE_FORMAT='KAFKA') as
select 
	rowkey,
	EXTEND_JSON(MAP(
		'_server_time' :=
		case
			when dtm > 0 then cast(dtm * 1000 as bigint)
			when dt > 0 then cast(dt as bigint) * 1000
			else UNIX_TIMESTAMP() 
		end,
		'_apptype_id' :=
		cast(case
			--backward compatibility
			when src_id between 148 and 150 then 11  --front androiod, beta, ios -> APPS
			when app = 17 and (src_id = 2 or src_id = 141 or src = 12) then 3 --API m.avito.ru -> MOBILE
			when app = 18 and src_id = 141 then 1 --web recommendations -> SITE
			--common case
			when src_id > 0 then src_id
			when src > 100 then (src % 100) 
			WHEN src < 100 then (src % 10)
			else 0
		end as bigint),
		'_clientsideapp_id' :=
		cast(case
			--backward compatibility
			when src_id = 148 then 3
			when src_id = 149 then 23
			when src_id = 150 then 1
			when app = 17 then 0
			when app = 18 and src_id = 141 then 0
			--common case
			when app > 0 then app
			else 0
		end as bigint),
		'_platform_id' := 
		CASE
			-- Avito.ru:
		    WHEN src_id = 1 THEN cast(1 as bigint)
		    WHEN src_id = 96 THEN cast(1 as bigint)
		    WHEN src_id is null and src = 11 THEN cast(1 as bigint) 
		    WHEN src_id = 141 and app = 18 then cast(1 as bigint) --web recommendations -> SITE
		    -- m.Avito.ru:
		    WHEN src_id = 3 THEN cast(2 as bigint)
		    WHEN src_id = 95 THEN cast(2 as bigint)
		    WHEN src_id is null and src = 13 THEN cast(2 as bigint)
		    when app = 17 and (src_id = 2 or src_id = 141 or src = 12) THEN cast(2 as bigint) --API m.avito.ru -> MOBILE
		    -- Android:
		    WHEN app = 3 THEN cast(3 as bigint)
		    WHEN src_id = 148 THEN cast(3 as bigint)
		    -- iOS:
		    WHEN app = 1 THEN cast(4 as bigint)
		    WHEN src_id = 150 THEN cast(4 as bigint)
		    -- adm.Avito.ru:
		    WHEN src_id = 4 THEN cast(5 as bigint)
		    WHEN src_id is null and src > 100 and src % 100 = 4 THEN cast(5 as bigint)
		    WHEN src_id is null and src < 100 and src % 10 = 4 THEN cast(5 as bigint)
		    -- Autoload:
		    WHEN src_id = 6 THEN cast(6 as bigint)
		    WHEN src_id is null and src > 100 and src % 100 = 6 THEN cast(6 as bigint)
		    WHEN src_id is null and src < 100 and src % 10 = 6 THEN cast(6 as bigint)
		    -- Android X:
		    WHEN app = 23 THEN cast(7 as bigint)
		    WHEN src_id = 149 THEN cast(7 as bigint)
		    -- AvitoPRO:
		    WHEN src_id = 12 THEN 16547000007 
		    WHEN src_id = 209 THEN 16547000007 
		    WHEN src_id is null and src > 100 and src % 100 = 12 THEN 16547000007 
		    -- Messenger Api:
		    WHEN src_id = 163 THEN cast(8 as bigint)
			else cast(0 as bigint)
		END,
		'_ua_is_test' := 
		cast(case 
			when ua is not null and ua like '%AvitoLoadTest%' then 1 
			else 0 
		end as bigint)--,
		--'_microcat_id' :=
		--cast(case 
		--	when eid = 312 then coalesce(microcat_node(cid, params), '0') 
		--	when eid = 300 then coalesce(microcat_node(cid, infm_raw_params), '0') 
		--	else '0' 
		--end as bigint)
		), `$`)
from clickstream_flat cs
PARTITION BY cs.ROWKEY
emit changes;

