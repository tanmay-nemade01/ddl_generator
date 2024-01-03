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
            concat_sta_hash.append('NVL((to_decimal('+str(name_list[i])+','+str(type_list[i][8:])+'),0),')
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

script_template = '''USE ROLE <sio>_CDP_OFFICER;
USE WAREHOUSE <wh>;
USE DATABASE <env>_CDP_CMACGM;
USE SCHEMA <SF_source>;
-----------------------PREPARTION FOR RAW TO SFS--------
ALTER TASK <env>_CDP_CMACGM.<SF_source>.TS_<SF_source>_<object name>_INGEST_MG SUSPEND;

ALTER PIPE <env>_CDP_CMACGM.<SF_source>_LANDING.SP_<object name> SET PIPE_EXECUTION_PAUSED = TRUE;

---------------------- STRUCTURES --------------------------------

-----Take backup of CDC table

CREATE OR REPLACE TABLE CDC_<object name>_BK CLONE <SF_source>_LANDING.CDC_<object name>;

---Take backup of processed table

CREATE OR REPLACE TABLE <object name>_BK CLONE <object name>;

--sfs normalized script can be deployed

CREATE OR REPLACE TRANSIENT TABLE TRA_<object name>(
	<STA_DEF>
);

 
 -- Add Metadata  
ALTER table TRA_<object name> add column FILE_DATE integer;
ALTER table TRA_<object name> add column file_type varchar;
ALTER table TRA_<object name> add column OP varchar;
ALTER table TRA_<object name> add column FILE_ROWNUM integer;


--create or replace table <object name> LIKE TRA_<object name>;
-- create or replace transient table CDC_<object name> LIKE TRA_<object name>; 
CREATE OR REPLACE TRANSIENT TABLE CDC_<object name>(
	<CDC_DEF>
);

 
/* ALTER TABLE <object name> drop column OP;
 ALTER TABLE <object name> drop column file_type;*/

 -- Activate Change tracking for the stream on CDC table
ALTER TABLE CDC_<object name> SET CHANGE_TRACKING = TRUE;
 
-- add others METADATA on CDC
ALTER table CDC_<object name> add column FILE_DATE integer;
ALTER table CDC_<object name> add column file_type varchar;
ALTER table CDC_<object name> add column OP varchar;
ALTER table CDC_<object name> add column FILE_ROWNUM integer;
ALTER table CDC_<object name> add column ROWID integer;
ALTER table CDC_<object name> add column F_VALID integer;
ALTER table CDC_<object name> add column RUN_DATE timestamp;

 ALTER TABLE <object name> ADD column file_DATE NUMBER(38,0);

-- Create a sequence for CDC table (rowid)
create or replace sequence SEQ_<object name> start = 1 increment = 1;

create or replace TABLE CDC_<object name>_TEST LIKE CDC_<object name>;
create or replace TABLE <object name>_TEST LIKE <object name>;

-- Create a stream for CDC to target table (processed)
create or replace stream ST_<SF_source>_<object name>_CDC on table CDC_<object name>_TEST APPEND_ONLY = TRUE;
---------------------------------Manual Copy Into to Transient table----
COPY INTO TRA_<object name> FROM(
SELECT 
<STAGE_DEF>
NVL(translate(to_char(TO_TIMESTAMP(REGEXP_SUBSTR(METADATA$FILENAME,'[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'),'YYYYMMDD-HH24MISSFF9'),'YYYYMMDD-HH24MISSFF3'),'-','')::number,19700101000000000) as FILE_DATE,
substr(split_part(metadata$filename,'/',4),1,1) as FILE_TYPE,
	 $1:Op ,
      METADATA$FILE_ROW_NUMBER as FILE_ROWNUM-- Be careful at the case sensitivity
FROM  @<env>_CDP_CMACGM.<SF_source>.STG_S3_CDP_CMACGM_<SF_source>_<env>_R/awscdc_cds/LRA_SCE_M/<object name>/(file_format => 'FF_PARQUET'));

-------------------------------
INSERT INTO CDC_<object name>_TEST
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
		SELECT 
        -- #columns#
		<CDC_TARGET>	-- metadata
			FILE_DATE,
			'F' AS FILE_TYPE,
            FILE_ROWNUM,-- <change>
		  sha1_binary(upper(array_to_string(array_construct_compact(
		  -- hashing #columns#
		<STA_HASH>      -- Be careful there are not comma for the last column
		  ), '-'))) as RECORD_HASH
		from TRA_<object name>
		  where FILE_TYPE = 'L'--<change>
		),
		-- target table
		tgt_chg as (
		  select *, 'F' as file_type,0::integer as FILE_ROWNUM,
			sha1_binary(upper(array_to_string(array_construct_compact(
			-- hashing #columns#  
		<TARGET_HASH>       -- Be careful there are not comma for the last column
		  ), '-'))) as RECORD_HASH  
		  from <object name>_TEST   
		),
		max_date as ( SELECT MAX(FILE_DATE) FILE_DATE,<table_key> from CDC_<object name>_TEST GROUP BY  <table_key> ) -- #key#

		-- INSERT
		select src.* , 'I' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		case when (src.FILE_DATE >= COALESCE(md.FILE_DATE,0)) then 1 else 0 end as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		left join max_date md on <src-md>
-- #key#
		where not exists ( select 1 from tgt_chg tgt where <tgt-src>)   -- #key#

		union
		--UPDATE -- when hashing diff
		select src.* , 'U' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		case when src.FILE_DATE >= tgt.FILE_DATE then 1 else 0 end as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		inner join tgt_chg tgt on <tgt-src> -- #key#
		where tgt.RECORD_HASH != src.RECORD_HASH

		union
		--UPDATE -- when hashing egal
		select src.* , 'U' as OP , 
		 SEQ_<object name>.nextval as ROWID ,
		0 as F_VALID,
		current_timestamp as RUN_DATE
		from src_chg src 
		inner join tgt_chg tgt on <src__tgt> -- #key#
		where tgt.RECORD_HASH = src.RECORD_HASH

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
    SELECT 	-- #columns#
	<CDC_UNION>
    -- METADATA
    t.file_date,
    'D' AS FILE_TYPE,
    nvl(t.OP,'I'),
    t.FILE_ROWNUM,
    SEQ_<object name>.nextval as ROWID ,
    case when t.FILE_DATE >= coalesce(t3.FILE_DATE, 0) then 1 else 0 end F_VALID,
    current_timestamp as RUN_DATE
        from TRA_<object name> t	
        left join (SELECT MAX(FILE_DATE) FILE_DATE , <table_key> from CDC_<object name>_TEST GROUP BY  <table_key>) t3 on <t-t3> -- #key#
    where t.file_type != ('L') ;--<change>
--243124606    

    merge into <object name>_TEST t
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

-----Drop Transient table which created temporary




    SELECT COUNT(*) FROM TRA_<object name>;
    SELECT COUNT(*) FROM <object name>;
    SELECT COUNT(*) FROM CDC_<object name>;

        SELECT COUNT(*) FROM <object name>_TEST;
    SELECT COUNT(*) FROM CDC_<object name>_TEST;

    SELECT MAX(FILE_DATE) FROM CDC_<object name>_TEST;
    

    ALTER TABLE <object name>_TEST SWAP WITH <object name>;
    ALTER TABLE CDC_<object name>_TEST SWAP WITH CDC_<object name>;



    DROP TABLE TRA_<object name>;
     DROP TABLE CDC_<object name>_TEST;
 DROP TABLE <object name>_TEST;'''

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

    pk_src__tgt = ''
    for i in range(int(data['No of Primary Key'][0])):
        pk_src__tgt = pk_src__tgt + f'src.{primary_key_list[i]} = tgt.{primary_key_list[i]} and\n'

  
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
    script_template = script_template.replace('<src__tgt>',pk_src__tgt[:-5])
    script_template = script_template.replace('<t-t3>',pk_t_t3[:-5])
    script_template = script_template.replace('<s-t>',pk_s_t[:-5])
    script_template = script_template.replace('<table_key>',primary_keys)
    script_template = script_template.replace('<CDC_TARGET1>',copy_table)

    st.code(script_template)
