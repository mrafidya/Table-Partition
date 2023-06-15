create table tmp.is_mr_partition_SUBS_ACTIVATION
(
	subs_id BIGINT
	,mdn VARCHAR(180)
	,activation_date TIMESTAMP without time zone
	,partition_date date
)
with (appendonly=true, compresstype=zlib, compresslevel=3, orientation=column)
distributed by (SUBS_ID)
partition by range(partition_date)
(
	partition d_20210101 start ('2021-01-01') end ('2021-01-02')
	with (APPENDONLY=true, COMPRESSLEVEL=3, ORIENTATION=COLUMN, COMPRESSTYpe=zlib),
	partition d_20210102 start ('2021-01-02') end ('2021-01-03')
	with (APPENDONLY=true, COMPRESSLEVEL=3, ORIENTATION=COLUMN, COMPRESSTYpe=zlib),
	partition d_20210103 start ('2021-01-03') end ('2021-01-04')
	with (APPENDONLY=true, COMPRESSLEVEL=3, ORIENTATION=COLUMN, COMPRESSTYpe=zlib)
)

create table tmp.proc_fnc_log
(
proc_date varchar (256),func_name varchar (256),seqno varchar (256),description varchar (256)
)

select * from tmp.is_lk_partition_monitoring order by process_At desc

select tmp.is_mr_add_partition3('tmp.is_mr_partition_SUBS_ACTIVATION', 20210101, 20210102, 'd', 5)



CREATE OR REPLACE FUNCTION tmp.is_mr_add_partition3(prm_tbl text, prm_start integer, prm_end integer, prm_type text, compresslev integer)
	  RETURNS integer AS
	$BODY$
 	 	

DECLARE
   v_START                timestamp;
   v_END                  timestamp;
   v_FUNCTION_NAME        text := 'tmp.is_mr_add_partition3';
   v_DESCRIPTION          text;
   v_CMD                  text;
   v_SEQNO                integer := 0;
   v_PARTITION_NAME		  text;
   v_CNT				  integer;
   v_SCHEMA			      text;
   v_TABLE				  text;
   v_CNT_PARTITION			integer;
   v_COMPRESS             text;
   namapartisi 			text;

   /* PERHATIAN
   1. Sebelum menjalankan function ini pastikan, partisi tabel tidak memiliki sub partisi. Contoh : rpt_cc.rep_debit_adj_l3
   2. function ini tdk mengakomodir error, seperti error partisi is exists
   3. parameter prm_tbl diisi lengkap dg schemanya
   4. Contoh utk menjalankan :
      select tmp.is_f_add_partition3('dwh_cc.f_add_partition', 20160101, 20160105, 'd', 5) --compresslevel=5
      select tmp.is_f_add_partition3('dwh_cc.f_add_partition', 20170101, 20170331, 'm', 3) --compresslevel=3
      select tmp.is_f_add_partition3('dwh_cc.f_add_partition', 20170101, 20170331, 'mi',4) --to partition range (integer type)
   5. Partisi dengan kompresi
   */

BEGIN
     v_COMPRESS := 'with(appendonly=true, compresslevel='|| compresslev ||', orientation=column, compresstype=zlib)';
     v_START        := clock_timestamp();
     v_DESCRIPTION  :='Start '||v_FUNCTION_NAME||' : '||v_START;

     RAISE NOTICE   'Start % : %',v_FUNCTION_NAME, v_START;
     v_SEQNO        := v_SEQNO + 1;
	 namapartisi	:=to_date(prm_end::text,'yyyymmdd');

     INSERT INTO tmp.PROC_fnc_LOG(proc_date,func_name,seqno,description) values (v_START, v_FUNCTION_NAME,v_SEQNO, v_DESCRIPTION);
	--INSERT INTO tmp.is_lk_partition_monitoring(table_name,partition_name,process_at) values (prm_tbl, v_PARTITION_NAME,v_START); 
     ------------------------------BY DATE------------------------------

     if (prm_type='d') THEN
	 
	--insert into TMP.IS_MR_partition_SUBS_ACTIVATION
	--select * from
	--tmp.tm_act_jan2021 where activation_date::date between prm_start and prm_end;
	SELECT (prm_end::text::date - prm_start::text::date) INTO v_CNT;
	SELECT
			split_part(prm_tbl, '.', 1),
			split_part(prm_tbl, '.', 2)
		INTO
		v_SCHEMA,
		v_TABLE;

	FOR i in 0..v_CNT
	LOOP
		select count(*)
		from pg_partitions
		WHERE  tablename = v_TABLE
		and partitionname != ''
		and partitiontype = 'range'
		and partitionschemaname = v_SCHEMA
		--and substring(partitionrangestart,2,10) between  prm_start::text::date+i and  prm_start::text::date+i+1
		and substring(partitionrangestart,2,10)::date between  prm_start::text::date+i and  prm_start::text::date+i+1	--update gp5 roi 20190411
		into v_CNT_PARTITION;

		IF (v_CNT_PARTITION) > 0
		THEN

		select partitionname
		from pg_partitions
		WHERE  tablename = v_TABLE
		and partitionname != ''
		and partitiontype = 'range'
		and partitionschemaname = v_SCHEMA
		--and substring(partitionrangestart,2,10) between  prm_start::text::date+i and  prm_start::text::date+i+1
		and substring(partitionrangestart,2,10)::date between  prm_start::text::date+i and  prm_start::text::date+i+1	--update gp5 roi 20190411
		into v_PARTITION_NAME;

		EXECUTE 'ALTER TABLE ' || prm_tbl || ' DROP PARTITION ' || v_PARTITION_NAME || '';
		SELECT prm_type || '_' || to_char((prm_start::text::date+i),'YYYYMMDD') INTO v_PARTITION_NAME;

		v_CMD := 'ALTER TABLE ' || prm_tbl || ' ADD PARTITION ' || v_PARTITION_NAME || ' START (''' || prm_start::text::date+i || ''') END (''' || prm_start::text::date+i+1 || ''')' || v_COMPRESS;
		RAISE NOTICE   '- % ',v_CMD;

		ELSIF (v_CNT_PARTITION) = 0
		THEN
		    SELECT prm_type || '_' || to_char((prm_start::text::date+i),'YYYYMMDD') INTO v_PARTITION_NAME;
		    v_CMD := 'ALTER TABLE ' || prm_tbl || ' ADD PARTITION ' || v_PARTITION_NAME || ' START (''' || prm_start::text::date+i || ''') END (''' || prm_start::text::date+i+1 || ''')' || v_COMPRESS;
		    RAISE NOTICE   '- % ',v_CMD;
		END IF;
		v_SEQNO:=v_SEQNO+1;
		INSERT INTO tmp.PROC_fnc_LOG(proc_date,func_name,seqno,description) values (v_START, v_FUNCTION_NAME,v_SEQNO, v_CMD); --log

		EXECUTE v_CMD;
	END LOOP;

     end if;

     ------------------------------BY MONTH------------------------------

     if (prm_type='m') THEN

	select (date_part('year', prm_end::text::date) - date_part('year', prm_start::text::date)) * 12 +
           (date_part('month', prm_end::text::date) - date_part('month', prm_start::text::date))
      INTO v_CNT;

	FOR i in 0..v_CNT
	LOOP
		SELECT prm_type || '_' || to_char((prm_start::text::date + (i||' MONTH')::INTERVAL)::date,'YYYYMM') INTO v_PARTITION_NAME;
		v_CMD := 'ALTER TABLE ' || prm_tbl || ' ADD PARTITION ' || v_PARTITION_NAME || ' START (''' || (prm_start::text::date + (i||' MONTH')::INTERVAL)::date || '''::date) END (''' || (prm_start::text::date + (i+1||' MONTH')::INTERVAL)::date || '''::date)' || v_COMPRESS;
		RAISE NOTICE   '- % ',v_CMD;

		v_SEQNO:=v_SEQNO+1;
		INSERT INTO tmp.PROC_fnc_LOG(proc_date,func_name,seqno,description) values (v_START, v_FUNCTION_NAME,v_SEQNO, v_CMD); --log

		EXECUTE v_CMD;
	END LOOP;

     end if;

     ------------------------------BY MONTH INT------------------------------

     if (prm_type='mi') THEN

	select (date_part('year', to_date(prm_end::text,'yyyymmdd')) - date_part('year', to_date(prm_start::text,'yyyymmdd'))) * 12 +
           (date_part('month', to_date(prm_end::text,'yyyymmdd')) - date_part('month', to_date(prm_start::text,'yyyymmdd')))
      INTO v_CNT;

	FOR i in 0..v_CNT
	LOOP
		SELECT 'm_' || to_char((to_date(prm_start::text,'yyyymmdd') + (i||' MONTH')::INTERVAL)::date,'YYYYMM') INTO v_PARTITION_NAME;
		v_CMD := 'ALTER TABLE ' || prm_tbl || ' ADD PARTITION ' || v_PARTITION_NAME || ' START (' || to_char(to_date(prm_start::text,'yyyymmdd') + (i||' MONTH')::INTERVAL,'yyyymmdd') || ') END (' || to_char(to_date(prm_start::text,'yyyymmdd') + (i+1||' MONTH')::INTERVAL,'yyyymmdd') || ')' || v_COMPRESS;
		RAISE NOTICE   '- % ',v_CMD;

		v_SEQNO:=v_SEQNO+1;
		INSERT INTO tmp.PROC_fnc_LOG(proc_date,func_name,seqno,description) values (v_START, v_FUNCTION_NAME,v_SEQNO, v_CMD); --log
		INSERT INTO tmp.is_lk_partition_monitoring(table_name,partition_name,process_at) values (prm_tbl, namapartisi,v_START); --log

		EXECUTE v_CMD;
	END LOOP;

     end if;

     ------------------------------FINISH--------------------------------

     v_END         := clock_timestamp();
     v_DESCRIPTION :='Finish '||v_FUNCTION_NAME||' : '||v_END||' Duration : '||(v_END - v_START)::interval;
     RAISE NOTICE   '% ',v_DESCRIPTION;
     v_SEQNO        := v_SEQNO + 1;
     INSERT INTO tmp.PROC_fnc_LOG(proc_date,func_name,seqno,description) values (v_START, v_FUNCTION_NAME,v_SEQNO, v_DESCRIPTION);
	INSERT INTO tmp.is_lk_partition_monitoring(table_name,partition_name,total_record,process_at) values (prm_tbl, namapartisi, v_CNT_PARTITION,v_START); 

     RETURN 1;
EXCEPTION WHEN OTHERS THEN
     RAISE EXCEPTION '(%:%)', v_FUNCTION_NAME, sqlerrm;
     RETURN 0;
END

   
$BODY$
  LANGUAGE plpgsql VOLATILE;
