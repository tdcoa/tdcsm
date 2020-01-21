/*{{save:{siteid}.dbcinfo.csv}}*/
/*{{load:adlste_coa.stg_dat_dbcinfo}}*/
/*{{call:adlste_coa.sp_dat_dbcinfo()}}*/

Select '{account}' as Account_Name, '{siteid}' as Site_ID, d.* 
,'Initial Load' as Record_Status  
,0 as Process_ID
,Current_Timestamp(0) as Process_TS
from dbc.dbcinfo as d
;
