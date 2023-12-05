
import streamlit as st

table_name = st.text_input('Enter table name')
primary_keys = st.text_input('Enter Primary Keys, seperated by commas if multiple')
primary_keys = primary_keys.upper()
primary_keys = primary_keys.replace(' ','')
primary_key_list = primary_keys.split(',')

primary_keys_stage = ''
for i in range(len(primary_key_list)):
    primary_keys_stage = primary_keys_stage + '$1:'+primary_key_list[i]+','

primary_keys_stage = primary_keys_stage[:-1]

table_count = 'select count(*) from ' + table_name +';'
stage_count = 'select count(distinct'+primary_keys_stage+') from @STG_S3_CDP_CMACGM_LARA_PRD_R/awscdc_cds/LRA_SCE_M/'+table_name+"/  (FILE_FORMAT => 'FF_PARQUET'));"
op_d_count = 'select count(distinct'+primary_keys_stage+') from @STG_S3_CDP_CMACGM_LARA_PRD_R/awscdc_cds/LRA_SCE_M/'+table_name+"/  (FILE_FORMAT => 'FF_PARQUET')) where $1:OP = 'D';"
table_count
stage_count
op_d_count
