import streamlit as st
import pandas as pd

schema = st.text_input('Enter Schema')   
table_name = st.text_input('Enter table name')
aws_url = st.text_input('Enter AWS link')
# primary_keys = st.text_input('Enter Primary Keys, seperated by commas if multiple')
# primary_keys = primary_keys.upper()
# primary_keys = primary_keys.replace(' ','')
# primary_keys

def main_function(json_data, table_name, aws_url, environment, schema):
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
        elif str(type_list[i])[:3].upper() == 'TIM':
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
    # primary_key_list = []
    # if ',' in primary_keys:
    #     primary_key_list = primary_keys.split(',')
    # else:
    #     primary_key_list = [primary_keys]

    table_name_list = [table_name]
    schema_list = [schema]
    environment_list = [environment]
    aws_list = [aws_url]
    # length1 = len(primary_key_list)
    # length1_list = [str(length1)]
    disregard_aws = [str(count)]

    for i in range(len(name_list)-1):
        table_name_list.append('X')
        schema_list.append('X')
        environment_list.append('X')
        aws_list.append('X')
        # length1_list.append('X')
        disregard_aws.append('X')


    
    
    # for i in range(len(name_list)-length1):
    #     primary_key_list.append('X')

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
        # 'Primary Keys':primary_key_list,
        # 'No of Primary Key':length1_list,
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

script_template = '''USE ROLE <sio>_CDP_OFFICER; --PROD PRD_CDP_OFFICER
USE WAREHOUSE <wh>;
USE DATABASE <ENV>_CDP_CMACGM;
USE SCHEMA <SF_source>;

CREATE OR REPLACE STAGE STG_CDP_<ENV>_<SF_source>_<object name> 
url = '<AWS_URL>'
STORAGE_INTEGRATION = SIO_CDP_<sio> -- SIO_ADMINTEST_NOPROD (UAT/DEV) 
-- Activate DIRECTORY TABLE
DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE) ;




-- Stream on directory table to detect a (or many) new files 
-- Be careful at the init, files already present are not detect, we need a new file at the bucket to init the stream. Or run the queries of the task in the right order
CREATE OR REPLACE STREAM ST_<SF_source>_<object name>_SQS ON STAGE STG_CDP_<ENV>_<SF_source>_<object name>;

--------------------------Creation of Process Table-----------
-- V1.1-- NO change in MAX_DATA_EXTENSION_TIME_IN_DAYS
CREATE OR REPLACE TABLE <object name>(
<CDC_DEF>
FILE_DATE INT)
  DATA_RETENTION_TIME_IN_DAYS = 1
  MAX_DATA_EXTENSION_TIME_IN_DAYS = 1
  CHANGE_TRACKING = TRUE;

--Create a table from STA table to don t forhget copy inot story  V1.1( based on CDP schedule MAX_DATA_EXTENSION_TIME_IN_DAYS may change)
CREATE OR REPLACE TABLE STA_<object name>(
<CDC_DEF>
FILE_DATE INT,
FILE_TYPE VARCHAR(16777216))
  DATA_RETENTION_TIME_IN_DAYS = 1
  MAX_DATA_EXTENSION_TIME_IN_DAYS = 2
  CHANGE_TRACKING = TRUE;
  
-- V1.1 to be able to swap
-- swap to avoid delete on processed table NO change in MAX_DATA_EXTENSION_TIME_IN_DAYS
CREATE OR REPLACE TABLE COPY_<object name>(
<CDC_DEF>
FILE_DATE INT)
  DATA_RETENTION_TIME_IN_DAYS = 1
  MAX_DATA_EXTENSION_TIME_IN_DAYS = 1
  CHANGE_TRACKING = TRUE;

-- create or replace stream ST_<SF_source>_<object name>_STA on table STA_<object name> APPEND_ONLY = TRUE;
-- Append only is removed due to AWS is genertaing multiple files at same time. In copy task we consume only part of data hence APPEND_ONLY is removed

create or replace stream ST_<SF_source>_<object name>_STA on table STA_<object name>;

--Checking data
SELECT
<STAGE_DEF>
replace(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+'),'-')::number + split_part(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+-\\\d+'),'-',4)::number as FILE_DATE 
from @STG_CDP_<ENV>_<SF_source>_<object name> (FILE_FORMAT => 'FF_PARQUET') limit 10;



---------------------------------------------------------------------
-- Mandatory : Create task consuming the to WRKorary table
-- -------------------------------------------------------------------- 

CREATE OR REPLACE TASK TS_<SF_source>_<object name>_SQS
  WAREHOUSE = <ENV>_CDP_L_S_VW
  SCHEDULE = '15 minutes'
  when system$stream_has_data('ST_<SF_source>_<object name>_SQS') 
  as select 1 ; 



   CREATE or replace TASK TS_<SF_source>_<object name>_DDL_CHECKING
 warehouse = <ENV>_CDP_L_S_VW 
 after TS_<SF_source>_<object name>_SQS
 as
 declare l_message string;
 begin
      l_message:=(call ADMIN_INGEST_FLOWS.PR_CALL_CDP_INTEGRITY('<ENV>','<object name>','<SF_source>','UPPER','<nb_columns_discrigard_aws>','1','TRUE'));
      
      if (l_message ='SUCCESS' or l_message ='WARNING' ) --if(l_message in ('SUCCESS','WARNING' )
      THEN
          DELETE FROM ADMIN_INGEST_FLOWS.SQS_CDP_INGEST WHERE RELATIVE_PATH in (SELECT RELATIVE_PATH FROM ST_<SF_source>_<object name>_SQS);
      ELSE
		  alter task TS_<SF_source>_<object name>_SQS suspend;
          select * from DDL_ERROR;
      END IF;
  end;




CREATE or REPLACE TASK TS_<SF_source>_<object name>_STA 
	WAREHOUSE = <ENV>_CDP_L_S_VW
	after TS_<SF_source>_<object name>_DDL_CHECKING
	as	
COPY INTO  STA_<object name> FROM 
(SELECT 
<STAGE_DEF>
replace(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+'),'-')::number + split_part(REGEXP_SUBSTR(METADATA$FILENAME,'-\\\d+-\\\d+-\\\d+'),'-',4)::number as FILE_DATE,
replace(REGEXP_SUBSTR(METADATA$FILENAME,'\\\w-'),'-') as file_type
from @STG_CDP_<ENV>_<SF_source>_<object name> (FILE_FORMAT => 'FF_PARQUET'))
pattern = '.*[\\/][FAD]-[0-9]{14}-[0-9]{3}-[0-9]+[.].*'  
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;
--ST_<SF_source>_<object name>_STA
-- V1.1
CREATE or REPLACE TASK TS_<SF_source>_<object name>_COPY 
	WAREHOUSE = <ENV>_CDP_L_S_VW
	after TS_<SF_source>_<object name>_STA
	when
	system$stream_has_data('ST_<SF_source>_<object name>_STA')
	as EXECUTE IMMEDIATE $$
	declare l_f_type string;
	begin
	 l_f_type := (select file_type from STA_<object name> where file_type='F' limit 1);
        if(l_f_type='F') THEN
		INSERT INTO  COPY_<object name> 
		(SELECT 
		<copy_table>
		FILE_DATE 
		from ST_<SF_source>_<object name>_STA where FILE_DATE>= NVL((select max(file_date) from STA_<object name> where FILE_TYPE ='F'),29991231000000000) );
		else
					select 1 from NO_FULL_LOAD_FILE;
		END IF;
	end;
	$$;


--Create task for SWAP V1.1 
CREATE or REPLACE TASK TS_<SF_source>_<object name>_SWAP
	WAREHOUSE = <ENV>_CDP_L_S_VW
    after TS_<SF_source>_<object name>_COPY
as
alter table  <object name> swap with  COPY_<object name>; 


--Create task for to delete and truncate table
CREATE or REPLACE TASK TS_<SF_source>_<object name>_DEL
	WAREHOUSE = <ENV>_CDP_L_S_VW
    after TS_<SF_source>_<object name>_SWAP
as
begin
delete from  STA_<object name>;
truncate table  COPY_<object name>;
end;


CREATE OR REPLACE TASK TS_<SF_source>_<object name>_ERR
  WAREHOUSE = <ENV>_CDP_L_S_VW  
  after TS_<SF_source>_<object name>_STA
  as insert into ADMIN_INGEST_FLOWS.ERR_CDP_INGEST
		-- Wrong pattern
		select distinct '<SF_source>' as schema_name, '<object name>' as table_name, METADATA$FILENAME as FILE_NAME , 'Not Loaded, wrong pattern' as status, current_timestamp as err_load_timestamp
		from @STG_CDP_<ENV>_<SF_source>_<object name>
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
		from @STG_CDP_<ENV>_<SF_source>_<object name> 
		where METADATA$FILE_ROW_NUMBER < 1 and replace(REGEXP_SUBSTR(METADATA$FILENAME,'\\\w-'),'-') = 'F'
			  and METADATA$FILENAME not in ( select distinct file_name from ADMIN_INGEST_FLOWS.ERR_CDP_INGEST );



--Task activation
alter task TS_<SF_source>_<object name>_ERR resume;
alter task TS_<SF_source>_<object name>_DEL resume;
alter task TS_<SF_source>_<object name>_SWAP resume;
alter task TS_<SF_source>_<object name>_COPY resume;
alter task TS_<SF_source>_<object name>_STA resume;
alter task TS_<SF_source>_<object name>_DDL_CHECKING resume;
alter task TS_<SF_source>_<object name>_SQS resume;

--Execute task immediately
execute task TS_<SF_source>_<object name>_STA;



--Checking task status
select * from table (information_schema.task_history(task_name=>'TS_<SF_source>_<object name>_TL1'));'''

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
    data = main_function(data, table_name, aws_url, environment, schema)

    # primary_key_list = data['Primary Keys']
    # primary_key_list = primary_key_list[0:int(data['No of Primary Key'][0])]
    # primary_keys = ','.join(primary_key_list)

    # primary_key_not_null = ''
    # null_replacement = data['NULL Replacement']
    name_list = list(data['Column Name'])

    # for i in range(int(data['No of Primary Key'][0])):
    #     primary_key_not_null = primary_key_not_null + f'ALTER table STA_<object name> MODIFY COLUMN {primary_key_list[i]} SET NOT NULL;\n'

    copy_table = ''
    for i in range(len(name_list)):
        copy_table = copy_table + name_list[i] +',\n'

    # pk_src_md = ''
    # for i in range(int(data['No of Primary Key'][0])):
    #     pk_src_md = pk_src_md + f'nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(md.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    # pk_tgt_src = ''
    # for i in range(int(data['No of Primary Key'][0])):
    #     pk_tgt_src = pk_tgt_src + f'nvl(tgt.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    # pk_src_tgt = ''
    # for i in range(int(data['No of Primary Key'][0])):
    #     pk_src_tgt = pk_src_tgt + f'nvl(src.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(tgt.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    # pk_t_t3 = ''
    # for i in range(int(data['No of Primary Key'][0])):
    #     pk_t_t3 = pk_t_t3 + f'nvl(t.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(t3.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    # pk_s_t = ''
    # for i in range(int(data['No of Primary Key'][0])):
    #     pk_s_t = pk_s_t + f'nvl(s.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) = nvl(t.{primary_key_list[i]},{null_replacement[name_list.index(primary_key_list[i])]}) and\n'

    # script_template = script_template.replace('<NOT_NULL>',str(primary_key_not_null))

    sta_creation_string = '\n'.join(list(data['Table Creation STA']))[:-1]
    script_template = script_template.replace('<STA_DEF>',str(sta_creation_string))

    cdc_creation_string = '\n'.join(list(data['Table Creation CDC/Processed']))
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

    script_template = script_template.replace('<ENV>',data['Environment'][0])
    script_template = script_template.replace('<sio>',sio)
    script_template = script_template.replace('<wh>',wh)
    script_template = script_template.replace('<SF_source>',data['Schema'][0])
    script_template = script_template.replace('<object name>',data['Table Name'][0])
    script_template = script_template.replace('<AWS_URL>',data['AWS'][0])
    script_template = script_template.replace('<nb_columns_discrigard_aws>',data['Disregard AWS'][0])
    # script_template = script_template.replace('<src-md>',pk_src_md[:-5])
    # script_template = script_template.replace('<src-tgt>',pk_src_tgt[:-5])
    # script_template = script_template.replace('<tgt-src>',pk_tgt_src[:-5])
    # script_template = script_template.replace('<t-t3>',pk_t_t3[:-5])
    # script_template = script_template.replace('<s-t>',pk_s_t[:-5])
    # script_template = script_template.replace('<table_key>',primary_keys)
    script_template = script_template.replace('<copy_table>',copy_table)

    st.code(script_template)
