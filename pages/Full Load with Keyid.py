import streamlit as st
import pandas as pd

schema = st.text_input('Enter Schema')
schema = schema.upper()
schema = schema.replace(' ','')
table_name = st.text_input('Enter table name')
table_name = table_name.upper()
table_name = table_name.replace(' ','')
aws_url = st.text_input('Enter AWS link')
aws_url = aws_url.replace(' ','')
primary_keys = st.text_input('Enter Primary Keys, seperated by commas if multiple')
primary_keys = primary_keys.upper()
primary_keys = primary_keys.replace(' ','')
primary_keys

def main_function(json_data, table_name, aws_url, environment, schema, primary_keys):
    # Load data from JSON file
    data = pd.DataFrame(json_data)

    # Specify the columns you want to extract
    length = len(data['COLUMN_NAME'])

    name_list = []
    for i in range(length):
        name_list.append(data['COLUMN_NAME'][i].upper())

    count = 0


    type_list = []
    for i in range(length):
        type_list.append(data['TYPE'][i].upper())

    if 'OP' in name_list:
        index = name_list.index('OP')
        type_list.pop(index)
        name_list.remove('OP')
        length -= 1
        count += 1


    if 'CDP_INGEST_DATETIME' in name_list:
        index = name_list.index('CDP_INGEST_DATETIME')
        type_list.pop(index)
        name_list.remove('CDP_INGEST_DATETIME')
        length -= 1
        count += 1

    st.write(f'No. of columns extra in AWS = {count}')

    for i in range(len(type_list)):
        if type_list[i] == 'TEXT':
            type_list[i] = 'STRING'
        if type_list[i] == 'TIMESTAMP_NTZ':
            type_list[i] = 'TIMESTAMP'
        

    string_list = []
    for i in range(length):
        string_list.append('STRING')

    sta_table_creation = []
    for i in range(length):
        sta_table_creation.append(str(name_list[i]) + ' ' + str(string_list[i])+',')

    cdc_table_creation = []
    for i in range(length):
        cdc_table_creation.append(str(name_list[i]) + ' ' + str(type_list[i])+',')

    stage_list = []
    for i in range(length):
        stage_list.append('$1:'+name_list[i]+'::'+'STRING,')

    cdc_target_list = []
    for i in range(length):
        if str(type_list[i])[:3].upper() == 'INT' or str(type_list[i])[:3].upper() == 'BIG':
            cdc_target_list.append('to_number('+str(name_list[i])+',38,0) as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'NUM':
            cdc_target_list.append('to_number('+str(name_list[i])+','+str(type_list[i][7:])+' as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'DEC':
            cdc_target_list.append('to_decimal('+str(name_list[i])+','+str(type_list[i][8:])+' as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'TIM':
            cdc_target_list.append(str(name_list[i])+'::timestamp as '+str(name_list[i])+',')
        else:
            cdc_target_list.append(str(name_list[i])+',')

    cdc_union = []
    for i in range(length):
        if str(type_list[i])[:3].upper() == 'INT' or str(type_list[i])[:3].upper() == 'BIG':
            cdc_union.append('to_number(t.'+str(name_list[i])+',38,0) as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'NUM':
            cdc_union.append('to_number(t.'+str(name_list[i])+','+str(type_list[i][7:])+' as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'DEC':
            cdc_union.append('to_decimal(t.'+str(name_list[i])+','+str(type_list[i][8:])+' as '+str(name_list[i])+',')
        elif str(type_list[i])[:3].upper() == 'TIM':
            cdc_union.append('t.'+str(name_list[i])+'::timestamp as '+str(name_list[i])+',')
        else:
            cdc_union.append('t.'+str(name_list[i])+',')

    null_replacement = []
    for i in range(length):
        if str(type_list[i])[:3].upper() == 'INT' or str(type_list[i])[:3].upper() == 'BIG':
            null_replacement.append('0')
        elif str(type_list[i])[:3].upper() == 'NUM':
            null_replacement.append('0')
        elif str(type_list[i])[:3].upper() == 'DEC':
            null_replacement.append('0')
        elif str(type_list[i])[:3].upper() == 'FLO':
            null_replacement.append('0')
        elif str(type_list[i])[:3].upper() == 'TIM':
            null_replacement.append("'9999-12-31'")
        elif str(type_list[i])[:3].upper() == 'DAT':
            null_replacement.append("'9999-12-31'")
        else:
            null_replacement.append("''")

    concat_sta_hash = []
    for i in range(length):
        if str(type_list[i])[:3].upper() == 'INT' or str(type_list[i])[:3].upper() == 'BIG':
            concat_sta_hash.append('NVL((to_number('+str(name_list[i])+',38,0)),0),')
        elif str(type_list[i])[:3].upper() == 'NUM':
            concat_sta_hash.append('NVL((to_number('+str(name_list[i])+','+str(type_list[i][7:])+'),0),')
        elif str(type_list[i])[:3].upper() == 'DEC':
            concat_sta_hash.append('to_decimal('+str(name_list[i])+','+str(type_list[i][8:])+'),0),')
        elif str(type_list[i])[:3].upper() == 'TIM':
            concat_sta_hash.append('NVL(('+str(name_list[i])+"::timestamp),'9999-12-31'),")
        else:
            concat_sta_hash.append('NVL(('+str(name_list[i])+"),'Z'),")

    concat_target_hash = []
    for i in range(length):
        if str(type_list[i])[:3].upper() == 'INT' or str(type_list[i])[:3].upper() == 'BIG':
            concat_target_hash.append('NVL(('+str(name_list[i])+"),0),")
        elif str(type_list[i])[:3].upper() == 'NUM':
            concat_target_hash.append('NVL(('+str(name_list[i])+"),0),")
        elif str(type_list[i])[:3].upper() == 'DEC':
            concat_target_hash.append('NVL(('+str(name_list[i])+"),0),")
        elif str(type_list[i])[:3].upper() == 'TIM':
            concat_target_hash.append('NVL(('+str(name_list[i])+"),'9999-12-31'),")
        else:
            concat_target_hash.append('NVL(('+str(name_list[i])+"),'Z'),")

    merge_join = []
    for i in range(length):
        merge_join.append('t.'+str(name_list[i])+' = s.'+str(name_list[i])+',')

    merge_union = []
    for i in range(length):
        merge_union.append('s.'+str(name_list[i])+',')

    glue = {'Column Name':name_list,
            'STA column type':string_list,
            'Column type':type_list,
            'Table Creation STA':sta_table_creation,
            'Table Creation CDC/Processed':cdc_table_creation}

    stage = {'Column Name':name_list,
            'Column type':type_list,
            'Stage column type':string_list,
            'stage':stage_list}

    cdc = {'Column Name':name_list,
            'Column type':type_list,
            'CDC Target':cdc_target_list,
            'CDC Union':cdc_union}

    hash = {'Column Name':name_list,
            'Column type':type_list,
            'Concat Target Hash':concat_target_hash,
            'Concat STA Hash':concat_sta_hash}

    merge = {'Column Name':name_list,
            'Join':merge_join,
            'Merge U':merge_union}

    glue = pd.DataFrame(glue)
    stage = pd.DataFrame(stage)
    cdc = pd.DataFrame(cdc)
    hash = pd.DataFrame(hash)
    merge = pd.DataFrame(merge)
    primary_key_list = []
    if ',' in primary_keys:
        primary_key_list = primary_keys.split(',')
    else:
        primary_key_list = [primary_keys]

    table_name_list = [table_name]
    schema_list = [schema]
    environment_list = [environment]
    aws_list = [aws_url]
    length1 = len(primary_key_list)
    length1_list = [str(length1)]
    disregard_aws = [str(count)]

    for i in range(len(name_list)-1):
        table_name_list.append('X')
        schema_list.append('X')
        environment_list.append('X')
        aws_list.append('X')
        length1_list.append('X')
        disregard_aws.append('X')


    
    
    for i in range(len(name_list)-length1):
        primary_key_list.append('X')

    df = {'Column Name':name_list,
        'STA column type':string_list,
        'Column type':type_list,
        'Table Creation STA':sta_table_creation,
        'Table Creation CDC/Processed':cdc_table_creation,
        'Stage':stage_list,
        'CDC Target':cdc_target_list,
        'CDC Union':cdc_union,
        'Concat Target Hash':concat_target_hash,
        'Concat STA Hash':concat_sta_hash,
        'Join':merge_join,
        'Merge U':merge_union,
        'Primary Keys':primary_key_list,
        'No of Primary Key':length1_list,
        'Table Name':table_name_list,
        'Schema': schema_list,
        'Environment':environment_list,
        'AWS':aws_list,
        'Disregard AWS':disregard_aws,
        'NULL Replacement': null_replacement
         }
    
    df = pd.DataFrame(df)
    st.dataframe(df)
    return df

script_template = '''USE ROLE <sio>_CDP_OFFICER;--FOR PROD PRD_CDP_OFFICER
USE WAREHOUSE <wh>;
USE DATABASE <env>_CDP_CMACGM;

USE SCHEMA <SF_source>;

---------------------- STRUCTURES --------------------------------

-- One stage for one table 
CREATE OR REPLACE STAGE STG_CDP_<env>_<SF_source>_<object name> 
url = '<AWS_URL>'
STORAGE_INTEGRATION = SIO_CDP_<sio> -- SIO_ADMINTEST_NOPROD (UAT/DEV) 
-- Activate DIRECTORY object name
DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE) ;

-- Stream on directory table to detect a (or many) new files 
-- Be careful at the init, files already present are not detect, we need a new file at the bucket to init the stream. Or run the queries of the task in the right order
CREATE OR REPLACE STREAM ST_<SF_source>_<object name>_SQS ON STAGE STG_CDP_<env>_<SF_source>_<object name>;

CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET
	TYPE = PARQUET
	NULL_IF = ()
;
 

-- Transiant like TEMPORARY / no history  All attributes should be created as String type
create or replace transient table STA_<object name>
 (
	<STA_DEF>	
);
 

 -- Add Metadata  
ALTER table STA_<object name> add column FILE_DATE integer;
ALTER table STA_<object name> add column file_type varchar;
ALTER table STA_<object name> add column OP varchar;
ALTER table STA_<object name> add column FILE_ROWNUM integer;

<NOT_NULL>

 --- Creation of CDC table
create or replace transient table CDC_<object name>
 (
	<CDC_DEF>	
);
 create or replace table <object name> LIKE CDC_<object name>;
 
ALTER TABLE <SF_source>.<object name> set tag ADMIN_INGEST_FLOWS.LOAD_TYPE= 'FULL_LOAD';
 
 -- Activate Change tracking for the stream on CDC table
ALTER table CDC_<object name> SET CHANGE_TRACKING = TRUE;
 
-- add others METADATA on CDC
ALTER table CDC_<object name> add column FILE_DATE integer;
ALTER table CDC_<object name> add column file_type varchar;
ALTER table CDC_<object name> add column OP varchar;
ALTER table CDC_<object name> add column FILE_ROWNUM integer;
ALTER table CDC_<object name> add column ROWID integer;
ALTER table CDC_<object name> add column F_VALID integer;
ALTER table CDC_<object name> add column RUN_DATE timestamp;

Alter table  CDC_<object name> set MAX_DATA_EXTENSION_TIME_IN_DAYS = 2;

-- add others METADATA on Processed
ALTER table <object name> add column FILE_DATE integer;


-- Create a sequence for CDC table (rowid)
create or replace sequence SEQ_<object name> start = 1 increment = 1;

-- Create a stream for CDC to target table (processed)
create or replace stream ST_<SF_source>_<object name>_CDC on table CDC_<object name> APPEND_ONLY = TRUE;



------------------------------ TASKS --------------------------------------------------------------
 CREATE OR REPLACE TASK TS_<SF_source>_<object name>_SQS
  WAREHOUSE = <env>_CDP_L_S_VW
  SCHEDULE = '15 minute'
  when system$stream_has_data('ST_<SF_source>_<object name>_SQS') 
  as select 1 ; 
  
 CREATE or replace TASK TS_<SF_source>_<object name>_DDL_CHECKING
 warehouse = <env>_CDP_L_S_VW 
 after TS_<SF_source>_<object name>_SQS
 as
 declare l_message string;
 begin
      l_message:=(call ADMIN_INGEST_FLOWS.PR_CALL_CDP_INTEGRITY('<env>','<object name>','<SF_source>','UPPER','<nb_columns_discrigard_aws>','1','FALSE'));
      
      if (l_message ='SUCCESS' or l_message ='WARNING' ) --if(l_message in ('SUCCESS','WARNING' )
      THEN
          DELETE FROM ADMIN_INGEST_FLOWS.SQS_CDP_INGEST WHERE RELATIVE_PATH in (SELECT RELATIVE_PATH FROM ST_<SF_source>_<object name>_SQS);
      ELSE
		  alter task TS_<SF_source>_<object name>_SQS suspend;
          select * from DDL_ERROR;
      END IF;
  end;

 CREATE OR REPLACE TASK TS_<SF_source>_<object name>_STA 
  WAREHOUSE = <env>_CDP_L_S_VW
after TS_<SF_source>_<object name>_DDL_CHECKING
  as 
copy into STA_<object name> from ( 
	SELECT 
	-- #columns# in SRC
	 <STAGE_DEF>
	-- METADATA
	 replace(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+'),'-')::number + split_part(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+-\\\d+'),'-',4)::number as FILE_DATE ,
	 replace(REGEXP_SUBSTR(METADATA$FILENAME,'\\\w-'),'-') as file_type,
	 $1:OP,-- Be careful at the case sensitivity
	  METADATA$FILE_ROW_NUMBER as FILE_ROWNUM
from @STG_CDP_<env>_<SF_source>_<object name> (file_format => 'FF_PARQUET')) 
pattern = '.*[\\/][FAD]-[0-9]{14}-[0-9]{3}-[0-9]+[.].*' 
on_error = 'ABORT_STATEMENT';


  CREATE OR REPLACE TASK TS_<SF_source>_<object name>_CDC
  WAREHOUSE = <env>_CDP_L_S_VW  
  after TS_<SF_source>_<object name>_STA
  as INSERT INTO CDC_<object name>
-- FULL --
	SELECT 
	-- #columns# target
		<CDC_TARGET1>
	-- Metadata
		FILE_DATE,
		file_type,
		OP,  
		FILE_ROWNUM,
		ROWID ,
		f_valid,
		RUN_DATE
	FROM 
		  (  
		 -- Stage 
		 with src_chg as (
		SELECT   -- #columns# Get it from excel CDC Target
			<CDC_TARGET>
		-- metadata
			FILE_DATE,
			file_type,
			FILE_ROWNUM,
		  sha1_binary(upper(array_to_string(array_construct_compact(
		  -- hashing #columns# Get it from excel sheet Hash
			<STA_HASH>        -- Be careful there are not comma for the last column
		  ), '-'))) as RECORD_HASH
		from STA_<object name>
		  where FILE_TYPE = 'F'
		),
		-- target table
		tgt_chg as (
		  select *, 'F' as file_type,0::integer as FILE_ROWNUM,
			sha1_binary(upper(array_to_string(array_construct_compact(
			-- hashing #columns# Get it from excel sheet Hash
			<TARGET_HASH>       -- Be careful there are not comma for the last column
		  ), '-'))) as RECORD_HASH  
		  from <object name>   
		),
		max_date as ( SELECT MAX(FILE_DATE) FILE_DATE , <table_key>  from CDC_<object name> GROUP BY  <table_key> ) -- #key#

		-- INSERT
		select src.* , 'I' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		case when (src.FILE_DATE >= COALESCE(md.FILE_DATE,0)) then 1 else 0 end as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		left join max_date md on <src-md> -- #key#
		where not exists ( select 1 from tgt_chg tgt where <tgt-src>)  -- #key#

		union
		--UPDATE -- when hashing diff
		select src.* , 'U' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		case when src.FILE_DATE >= tgt.FILE_DATE then 1 else 0 end as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		inner join tgt_chg tgt on <tgt-src> -- #key#
		where tgt.RECORD_HASH != src.RECORD_HASH

		/*union
		--UPDATE -- when hashing egal
		select src.* , 'U' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		0 as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		inner join tgt_chg tgt on tgt.<table_key> = src.<table_key> -- #key#
		where tgt.RECORD_HASH = src.RECORD_HASH
		*/

		union
		--DELETE
		select tgt.* , 'D' as OP, 
		 SEQ_<object name>.nextval as ROWID ,
		 1 as f_valid,
		 current_timestamp as RUN_DATE
		from tgt_chg tgt
		where not exists ( select 1 from src_chg src  where <src-tgt>) -- #key#
		and exists  (select 1 from src_chg src )
		)
-- CDC --
    UNION 
    SELECT 	-- #columns# Get it from excel CDC Target
	<CDC_UNION>
    -- METADATA
    t.file_date,
    t.file_type,
    nvl(t.OP,'I'), 
	t.FILE_ROWNUM,
    SEQ_<object name>.nextval as ROWID ,
    case when t.FILE_DATE >= coalesce(t3.FILE_DATE, 0) then 1 else 0 end F_VALID,
    current_timestamp as RUN_DATE
        from STA_<object name> t	
        left join (SELECT MAX(FILE_DATE) FILE_DATE , <table_key> from CDC_<object name> GROUP BY  <table_key> ) t3 on <t-t3> -- #key#
    where t.file_type in ('D','A') ;
	
	

-- task delete stage
CREATE OR REPLACE TASK TS_<SF_source>_<object name>_DEL
  WAREHOUSE = <env>_CDP_L_S_VW  
  after TS_<SF_source>_<object name>_CDC
  as DELETE FROM STA_<object name> ; 


-- Task to check error
CREATE OR REPLACE TASK TS_<SF_source>_<object name>_ERR
  WAREHOUSE = <env>_CDP_L_S_VW  
  after TS_<SF_source>_<object name>_CDC
  as insert into ADMIN_INGEST_FLOWS.ERR_CDP_INGEST
		-- Wrong pattern
		select distinct '<SF_source>' as schema_name, '<object name>' as table_name, METADATA$FILENAME as FILE_NAME , 'Not Loaded, wrong pattern' as status, current_timestamp as err_load_timestamp
		from @STG_CDP_<env>_<SF_source>_<object name>
		where not REGEXP_LIKE (METADATA$FILENAME , '.*[\\/][FAD]-[0-9]{14}-[0-9]{3}-[0-9]+[.].*')
			  and METADATA$FILENAME not in ( select distinct file_name from ADMIN_INGEST_FLOWS.ERR_CDP_INGEST )
		UNION
		-- Load error
		select '<SF_source>' as schema_name, '<object name>' as table_name, FILE_NAME, status || ' : ' || FIRST_ERROR_MESSAGE as status , current_timestamp as err_load_timestamp
		from table(information_schema.copy_history(table_name=>'STA_<object name>', start_time=> dateadd(day, -1, current_timestamp())))
		where status not in ('Loaded','Load in progress')  
		and FILE_NAME not in ( 
								select distinct FILE_NAME 
								from table(information_schema.copy_history(table_name=>'STA_<object name>', start_time=> dateadd(day, -1, current_timestamp())))
								where status in ('Loaded','Load in progress'))
	 	UNION
		-- Empty Full file error 
		select distinct '<SF_source>' as schema_name, '<object name>' as table_name, METADATA$FILENAME as FILE_NAME , 'Empty Full file error' as status, current_timestamp as err_load_timestamp
		from @STG_CDP_<env>_<SF_source>_<object name> 
		where METADATA$FILE_ROW_NUMBER < 1 and replace(REGEXP_SUBSTR(METADATA$FILENAME,'\\\w-'),'-') = 'F'
			  and METADATA$FILENAME not in ( select distinct file_name from ADMIN_INGEST_FLOWS.ERR_CDP_INGEST );



  CREATE OR REPLACE TASK TS_<SF_source>_<object name>
  WAREHOUSE = <env>_CDP_L_S_VW
  after TS_<SF_source>_<object name>_CDC
  when
	system$stream_has_data('ST_<SF_source>_<object name>_CDC')
  as merge into <object name> t
	using(select * from (select row_number() over(partition by <table_key>  order by FILE_DATE desc NULLS LAST,  FILE_ROWNUM desc NULLS LAST) rk,* from ST_<SF_source>_<object name>_CDC   where F_VALID = 1 ) where rk = 1 )s on <s-t> -- #key#
	when matched and s.op = 'D' and   s.FILE_DATE >= t.FILE_DATE then delete
	when matched and s.op in ('U','I') and   s.FILE_DATE >= t.FILE_DATE then update set
	-- #columns#
	<JOIN>
-- METADATA
		t.FILE_DATE =  s.FILE_DATE
	when not matched and s.op != 'D' then insert values (
	-- #columns#
	<MERGE> 
-- METADATA
		s.FILE_DATE
	);
	
	
------------------- RUN ---------------------------
	
ALTER TASK TS_<SF_source>_<object name>_DEL RESUME;
ALTER TASK TS_<SF_source>_<object name>_ERR RESUME;
ALTER TASK TS_<SF_source>_<object name> RESUME;
ALTER TASK TS_<SF_source>_<object name>_CDC RESUME;
ALTER TASK TS_<SF_source>_<object name>_STA RESUME;
ALTER TASK TS_<SF_source>_<object name>_DDL_CHECKING RESUME;
ALTER TASK TS_<SF_source>_<object name>_SQS RESUME;

select * from table (information_schema.task_history(task_name=>''));'''

file = st.file_uploader("Choose DDL file to upload")
environment = st.selectbox('Select Environment',('INT', 'UAT', 'PRD'))
if environment == 'INT' or environment == 'UAT':
	sio = 'NOPRD'
else:
	sio = 'PRD'

if environment == 'INT':
	wh = 'INT_CDP_L_B_VW'
elif environment == 'UAT':
	wh = 'UAT_CDP_L_B_VW'
else:
	wh = 'PRD_CDP_R_S_VW'
if file is not None:
    data  = pd.read_csv(file)
    data = main_function(data, table_name, aws_url, environment, schema, primary_keys)

    primary_key_list = data['Primary Keys']
    primary_key_list = primary_key_list[0:int(data['No of Primary Key'][0])]
    primary_keys = ','.join(primary_key_list)

    primary_key_not_null = ''
    null_replacement = data['NULL Replacement']
    name_list = list(data['Column Name'])

    copy_table = ''
    for i in range(len(name_list)):
        copy_table = copy_table + name_list[i] +',\n'

    script_template = script_template.replace('<CDC_TARGET1>',copy_table)

    for i in range(int(data['No of Primary Key'][0])):
        primary_key_not_null = primary_key_not_null + f'ALTER table STA_<object name> MODIFY COLUMN {primary_key_list[i]} SET NOT NULL;\n'

    pk_src_md = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_src_md = pk_src_md + f'nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(md.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    pk_tgt_src = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_tgt_src = pk_tgt_src + f'nvl(tgt.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    pk_src_tgt = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_src_tgt = pk_src_tgt + f'nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(tgt.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    pk_t_t3 = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_t_t3 = pk_t_t3 + f'nvl(t.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(t3.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    pk_s_t = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_s_t = pk_s_t + f'nvl(s.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(t.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    script_template = script_template.replace('<NOT_NULL>',str(primary_key_not_null))

    sta_creation_string = '\n'.join(list(data['Table Creation STA']))[:-1]
    script_template = script_template.replace('<STA_DEF>',str(sta_creation_string))

    cdc_creation_string = '\n'.join(list(data['Table Creation CDC/Processed']))[:-1]
    script_template = script_template.replace('<CDC_DEF>',str(cdc_creation_string))

    stage_string = '\n'.join(list(data['Stage']))
    script_template = script_template.replace('<STAGE_DEF>',str(stage_string))

    cdc_target_string = '\n'.join(list(data['CDC Target']))
    script_template = script_template.replace('<CDC_TARGET>',str(cdc_target_string))

    concat_target_string = '\n'.join(list(data['Concat Target Hash']))[:-1]
    script_template = script_template.replace('<TARGET_HASH>',str(concat_target_string))

    concat_sta_string = '\n'.join(list(data['Concat STA Hash']))[:-1]
    script_template = script_template.replace('<STA_HASH>',str(concat_sta_string))

    cdc_union_string = '\n'.join(list(data['CDC Union']))
    script_template = script_template.replace('<CDC_UNION>',str(cdc_union_string))

    join_string = '\n'.join(list(data['Join']))
    script_template = script_template.replace('<JOIN>',str(join_string))

    merge_string = '\n'.join(list(data['Merge U']))
    script_template = script_template.replace('<MERGE>',str(merge_string))

    script_template = script_template.replace('<env>',data['Environment'][0])
    script_template = script_template.replace('<sio>',sio)
    script_template = script_template.replace('<wh>',wh)
    script_template = script_template.replace('<SF_source>',data['Schema'][0])
    script_template = script_template.replace('<object name>',data['Table Name'][0])
    script_template = script_template.replace('<AWS_URL>',data['AWS'][0])
    script_template = script_template.replace('<nb_columns_discrigard_aws>',data['Disregard AWS'][0])
    script_template = script_template.replace('<src-md>',pk_src_md[:-5])
    script_template = script_template.replace('<src-tgt>',pk_src_tgt[:-5])
    script_template = script_template.replace('<tgt-src>',pk_tgt_src[:-5])
    script_template = script_template.replace('<t-t3>',pk_t_t3[:-5])
    script_template = script_template.replace('<s-t>',pk_s_t[:-5])
    script_template = script_template.replace('<table_key>',primary_keys)

    st.download_button(
	label="Download SQL Code",
        data=script_template,
	file_name=f"{environment}.{table_name}.sql",
	mime="text/plain"
	)
    st.code(script_template)

