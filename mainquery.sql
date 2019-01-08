--========================= Automate the Mortality Assumption =========================
--=====================================================================================

Use Mortality_Study

--=====================================================================================
--=================================== Update Section ==================================
-- Update 25/12/2018
-- Version 1.1 : Create the new Mortality Database to merge and config all data  

-- Adding the Term Rider in this Query

--=====================================================================================
--=================================== Update Table ====================================
-- Prophet Result: 201712 
-- Claim Data    : 
-- Prophet Table :
-- Spectial Case :


--=====================================================================================
-- List of tables request before running
-- Mort_Table
-- Prophet_Table
-- Prophet_Result
-- Claim Data
-- Prophet_Table_Fixed
--======================= Control to choose the Prophet_Result ========================

declare @Pre_data nvarchar(20)
declare @Cur_data nvarchar(20)
declare @Claim_data nvarchar(20)


Set @Pre_data = '_201612'       ---- !!!!! Change Here !!!!!         
Set @Cur_data = '_201712'       ---- !!!!! Change Here !!!!!
-- Set @Claim_data = 'CA_Database_201810'     ---- !!!!! Change Here !!!!!

-- Remark: After change the Control, Please Click to run all query to receive the result
--======================================================================================

-- For Merging all both database together

EXECUTE('
create view [Prophet_Merge] as

select *,''Pre_result'' as [Reslt_Date]
from Prophet_Result'+@Pre_data+'

union all


select *,''Cur_result'' as [Reslt_Date]
from Prophet_Result'+@Cur_data+'

go')


-- Create a Prophet result table that merge together 

--EXECUTE('
--select * into [Prophet_Merge_All'+@Pre_data+''+@Cur_data+']
--from [Prophet_Merge]

--go')

EXECUTE('
select * into [Prophet_Merge_All]
from [Prophet_Merge]

go')


-- Drop view
drop view [Prophet_Merge]

go


--======================= Fixed Prophet_Code before Jan2018 ============================
-- Using: Prophet_Table_Fixed

create view [Prophet_Table_New] as

select o.[DATA],o.Product,o.[SYSTEM_NAME / PRODUCT_PORT],o.[PACKAGE_CODE],o.[PLAN_COMPONENT],o.[SUB_CHANNEL],o.[MORT_TAB],o.[SUB_PLAN]
,IIF(x.filename is not null,x.Last_filename,o.[FILENAME]) as [FILENAME]
,o.[VALN_INT],o.[CSV_INT],o.[POL_TERM],o.[PRM_TERM],o.[LOAN_INT],o.[LOAN_INT_VAL],o.[ORIG_PACK_CODE],o.[SPCODE],o.[TL_VaLint_Ori],o.[comment],o.[Channel],o.[Sub_Channel Name],o.[Description],o.[Product_Type],
		o.[Port]
      ,o.[Type]
from Prophet_Table as o left join Prophet_Table_Fixed as x on( o.FILENAME = x.filename) 

go



--================================ Claim Data here     =================================


--=============================================================================================================================================================================================================
-- Create Indicator for mapping in Prophet Table!!

create view Claim_Mort1 as

select d.[Port_Type] as [M_Port_Type]
      ,d.[MASTER_POLICY] as [M_Master_Policy]
      ,d.[CERNO_POLNO] as [M_Cerno_polno]
      ,d.[PLAN_COMPONENT] as [M_PLAN_COMPONENT]
	  ,d.[PLAN_COMPONENT_02] as [M_PLAN_COMPONENT_02]
      ,d.[PACKAGE_CODE]  as [M_PACKAGE_CODE]
      ,d.[SUB_CHANNEL]   as [M_SUB_CHANNEL]
      ,d.[SUB_OFFICE]    as [M_SUB_OFFICE]
      ,d.[DEPENDENT_CODE] as [M_DEPENDENT_CODE],c.*
from [Claim extract] as c left join Map_Data as d on( 
CASE WHEN c.PRODUCT_TYPE ='GM' then C.CERTIFICATE_NO 
	 WHEN c.PRODUCT_TYPE = 'GR' Then C.GYRT_GR_CER
	 else  C.[CONTRACT_NO] end  = d.cerno_polno 

and iif( c.PRODUCT_TYPE = 'GM' , c.master_policy,'') = iif( c.PRODUCT_TYPE = 'GM' , d.master_policy,'')

and iif( c.PRODUCT_TYPE = 'GR' ,c.Package_Code,iif( c.PRODUCT_TYPE = 'GM' and c.Master_Policy like 'GCS%' ,'',iif(c.PRODUCT_TYPE = 'PA','' ,c.Rider_Code)))  = iif( c.PRODUCT_TYPE = 'GM' and c.Master_Policy like 'GCS%','',iif(c.PRODUCT_TYPE = 'PA','' ,d.[PLAN_COMPONENT] ) )
and iif( c.PRODUCT_TYPE = 'GR' ,'',iif(c.PRODUCT_TYPE = 'PA','' ,c.Package_Code ))  = iif( c.PRODUCT_TYPE = 'GR' ,'',iif(c.PRODUCT_TYPE = 'PA','' ,d.[PACKAGE_CODE] ))

and iif( c.PRODUCT_TYPE = 'GR' ,c.GYRT_GRP_TYPE,'')  = iif( c.PRODUCT_TYPE = 'GR' ,left(d.Master_Policy,2),'')  
and iif( c.PRODUCT_TYPE = 'GR' ,c.GYRT_GRP_pol,'')  = iif( c.PRODUCT_TYPE = 'GR' ,right(d.Master_Policy,6),'')  
and iif( c.PRODUCT_TYPE = 'GR' ,c.GYRT_SUB_OFF,'')  = iif( c.PRODUCT_TYPE = 'GR' ,d.SUB_OFFICE,'')  
and iif( c.PRODUCT_TYPE = 'GR' ,c.GYRT_DEP_COD,'')  = iif( c.PRODUCT_TYPE = 'GR' ,d.DEPENDENT_CODE,'')  


and iif( c.PRODUCT_TYPE = 'PA' ,c.Product_type,'')  = iif( c.PRODUCT_TYPE = 'PA' ,d.Port_Type,'')  
 )

where left(cast(c.NOTICE_DATE as numeric) ,4) = '2560'
----and c.PRODUCT_TYPE ='GR'
and c.CLAIM_TYPE ='DEATH'
and c.CLAIM_STS_DESC = 'PAID'

go





create view Claim_Mort2 as

select t.[filename],t.SPCODE,t.ORIG_PACK_CODE,c.*
from Claim_Mort1 as c left join [Prophet_Table] as t on( 
  -- Plan
  iif(c.M_port_Type = 'PA','Plan',iif(c.M_port_Type = 'GYRT','',c.[M_PLAN_COMPONENT])) = iif(c.M_port_Type = 'PA','Plan',iif(c.M_port_Type = 'GYRT','',t.[PLAN_COMPONENT])) 
  
  -- Sub_Channel
  AND iif(c.M_port_Type = 'GYRT','',CAST(c.[M_SUB_CHANNEL] AS NVARCHAR)) = iif(c.M_port_Type = 'GYRT','',CAST(t.[SUB_CHANNEL] AS NVARCHAR) )
  
  -- Packagecode
  -- AND iif(c.M_port_Type = 'PA','Pack',c.[M_PACKAGE_CODE]) = iif(c.M_port_Type = 'PA','Pack',t.[PACKAGE_CODE])
  AND iif(c.M_port_Type = 'PA','Pack',iif(c.M_port_Type = 'GYRT',c.M_Master_Policy,iif(c.M_port_Type = 'GMDT','',c.[M_PACKAGE_CODE]))) = iif(c.M_port_Type = 'PA','Pack',iif(c.M_port_Type = 'GMDT','',t.[PACKAGE_CODE]))

  -- Packagecode for GMDT 
  AND (( iif(c.M_port_Type = 'GMDT',c.M_Master_Policy,'') = iif(c.M_port_Type = 'GMDT',t.[PACKAGE_CODE],'') AND iif(c.M_port_Type = 'GMDT',c.[M_PLAN_COMPONENT_02],'') = iif(c.M_port_Type = 'GMDT',t.SUB_PLAN,'') )
       OR ( iif(c.M_port_Type = 'GMDT',c.[M_PACKAGE_CODE],'') = iif(c.M_port_Type = 'GMDT',t.[PACKAGE_CODE],'') AND iif(c.M_port_Type = 'GMDT',c.M_Master_Policy,'') = iif(c.M_port_Type = 'GMDT',t.SUB_PLAN,'') ) )
  -- Port_Type
  AND iif(c.M_Port_Type ='PA','PA',iif(c.M_Port_Type ='YODA','YODA',iif(c.M_Port_Type ='GRP','GRP_MODEL',''))) = iif(c.M_Port_Type ='PA',t.Data,iif(c.M_Port_Type='Yoda',t.Data,iif(c.M_Port_Type ='GRP',t.Data,'')))   )
--where c.PRODUCT_TYPE ='GM'
--order by c.M_Master_Policy,c.M_Cerno_polno

go




create view Claim_Mort3 as

select case when t.Last_Filename is not null then t.Last_Filename
	        else c.[filename] end as [FILENAME],c.[ORIG_PACK_CODE]
,c.[SPCODE],c.[M_Port_Type],c.[M_Master_Policy],c.[M_Cerno_polno],c.[M_PLAN_COMPONENT],c.[M_PLAN_COMPONENT_02],c.[M_PACKAGE_CODE],c.[M_SUB_CHANNEL],c.[M_SUB_OFFICE]
,c.[M_DEPENDENT_CODE],c.[SYSTEM_NAME],c.[RUNNING],c.[PRODUCT_TYPE],c.[CONTRACT_NO],c.[MASTER_POLICY],c.[CERTIFICATE_NO],c.[GYRT_GRP_TYPE],c.[GYRT_GRP_POL],c.[GYRT_SUB_OFF],c.[GYRT_GR_CER]
,c.[GYRT_DEP_COD],c.[INSURED_ID],c.[OCCUPATION_CODE],c.[CLAIM_YEAR],c.[CLM_NOTICE_NO],c.[ALT_CLAIM_TYPE],c.[ALT_CLAIM_YEAR],c.[ALT_CLAIM_NOTICE]
,c.[ALT_CLAIM_STS],c.[POLICY_STS],c.[CLAIM_STS],c.[CLAIM_STS_DESC],c.[CLAIM_TYPE_CODE],c.[CLAIM_TYPE],c.[PACKAGE_CODE],c.[PACKAGE_DESC],c.[RIDER_CODE]
,c.[RIDER_NAME],c.[DOI_DATE],c.[EFFECTIVE_DATE],c.[FST_ISSUE_DATE],c.[ISSUE_DATE],c.[EXPIRE_DATE],c.[DOC_RCV_DATE],c.[NOTICE_DATE],c.[CLM_APV_DATE],c.[CLM_PAYMENT_DATE]
,c.[CLAIM_INC_DATE],c.[CAUSE_OF_DTH_CODE],c.[DIAGNOSIS],c.[CAUSE_DESC],c.[CAUSE_OF_NUL_CODE],c.[NULLIFY_DESC],c.[IPD_OPD],c.[HOSPITAL]
,c.[ADMISSION_DATE],c.[DISCHARGE_DATE],c.[LEN_OF_STAY],c.[CURR],c.[TOTAL_CLM_AMT],c.[ACTUAL_CLM_AMT],c.[POL_UNPAID_PREMIUM],c.[POL_UNPAID_LOAN],c.[POL_UNPAID_LOAN_INT]
,c.[DEPOSIT_COND_BONUS],c.[PAID_SUM_INSURED],c.[CASH_VALUE],c.[PREMIUM_PAID],c.[CLM_PMT_MTD],c.[CLM_PMT_MTD_DESC],c.[BANK_CODE],c.[ACCOUNT_NO],c.[AGENT_CODE],c.[AGENT_NAME],c.[SVC_AGENT_CODE],c.[SVC_AGENT_NAME],c.[CHANNEL_CODE],c.[SUB_CHANNEL_CODE],c.[OWNER_NATIONAL_ID],c.[OWNER_NAME],c.[PAYEE_NAME],c.[LIFE_NO],c.[LIFE_NAME],c.[DATE_OF_BIRTH],c.[APPROVE_NAME]
,c.[GUSER],c.[GRUNDT],c.[GRUNTM],c.[PPO_FLAG],c.[BNF_CODE],c.[CLAIM_ID],c.[CLAIM_REG_ID],c.[CLAIM_ISS_AGE]
,IIF(c.[CLAIM_GENDER] ='M',0,1) as [CLAIM_GENDER],c.[SUM_INSURED],c.[HS_BILL_FOR_ICU],c.[BILL_ROOM_BROAD],c.[BILL_PHARMA],c.[BILL_MIS_EXPEND]
,c.[BILL_SURGICAL],c.[BILL_OPERATION_ROOM],c.[BILL_ANAESTHETIC],c.[BILL_MINOR_SURGICAL],c.[BILL_OPD_ACCIDENT],c.[BILL_X_RAYS_LAB]
,c.[BILL_AMBULANCE],c.[BILL_DOCTOR_VISIT],c.[BILL_OTHER],c.[HS_FOR_ICU],c.[ROOM_BROAD],c.[PHARMA],c.[MIS_EXPEND],c.[SURGICAL]
,c.[OPERATION_ROOM],c.[ANAESTHETIC],c.[MINOR_SURGICAL],c.[OPD_ACCIDENT],c.[X_RAYS_LAB],c.[AMBULANCE],c.[DOCTOR_VISIT],c.[OTHER],c.[EXTRACT_USER],c.[EXTRACT_YY],c.[EXTRACT_MM],c.[EXTRACT_DD],c.[EXTRACT_TIME],c.[Group_NON_Group]
from Claim_Mort2 as c left join Prophet_Table_Fixed as t on(c.filename = t.Filename)

go


--===================================================================================================================================================================================


--================================ Run the result here =================================
-- 5902876
------ Revised the coding for calculate the block of IF 

create view [Prophet_Mort] as

select t.[Product] as [Product_PLan],iif( o.[FILENAME] in('C_ETI_','C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__') ,k.PLAN_COMPONENT,NULL) as [Plan_Code_ETIRPU],t.[Channel],t.[Sub_Channel Name],t.port,
CASE WHEN  o.[FILENAME] = 'C_ETI_' THEN 'Basic'
	 WHEN  o.[FILENAME] = 'C_ETIL' THEN 'Basic'
	 WHEN  o.[FILENAME] = 'CTETI_' THEN 'Basic'
	 WHEN  o.[FILENAME] = 'C_PUP_' THEN 'Basic'
	 WHEN  o.[FILENAME] = 'C_PUPL' THEN 'Basic'
	 WHEN  o.[FILENAME] = 'CTPU__' THEN 'Basic' else t.[Type] end as [Type]
,t.[Description],t.[Product_Type], 

----iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Pre_result', 0 ,iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Cur_result'  ,m.[q_x+0],m.[q_x+0])) as [q_x+0],
m.[q_x+0],

----iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Pre_result', 0 ,iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Cur_result'  ,1,0.5)) as [Exposure_Case],
0.5 as [Exposure_Case],


----iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Pre_result', 0 ,iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Cur_result'  ,1*o.DEATH_BEN_PP ,0.5*o.DEATH_BEN_PP )) as [Exposure_SA],
o.DEATH_BEN_PP /2 as [Exposure_SA],

----iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Pre_result', 0 ,iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Cur_result'  ,1*m.[q_x+0] ,0.5*m.[q_x+0] )) as [Ex_Case],
m.[q_x+0]*1/2 as [Ex_Case],

----iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Pre_result', 0 ,iif(ENTRY_YEAR <=2016 and Reslt_Date = 'Cur_result'  ,1*m.[q_x+0]*o.DEATH_BEN_PP ,0.5*m.[q_x+0]*o.DEATH_BEN_PP )) as [Ex_SA],
m.[q_x+0]*o.DEATH_BEN_PP/2 as [Ex_SA],


o.*
from [Prophet_Merge_All] as o left join ( select Product,[FILENAME],[SPCODE],[Channel],[Sub_Channel Name],[Type],[Description],[Product_Type]
      ,[Port]
      
from [Prophet_Table_New] 

--where SPCODE is not null  


group by Product,[FILENAME],[SPCODE],[Channel],[Sub_Channel Name],[Type],[Description],[Product_Type],[Port]
       )as t on( o.[filename] = t.[filename]
--and  iif( t.[FILENAME] ='CTGE__',1,o.SPCODE) = t.SPCODE  ) 
and  case when t.[FILENAME] ='CTGE__' then 1 
		  WHEN  o.[FILENAME] = 'C_ETI_' THEN 1
	      WHEN  o.[FILENAME] = 'C_ETIL' THEN 1
	      WHEN  o.[FILENAME] = 'CTETI_' THEN 1
	      WHEN  o.[FILENAME] = 'C_PUP_' THEN 1
	      WHEN  o.[FILENAME] = 'C_PUPL' THEN 1
	      WHEN  o.[FILENAME] = 'CTPU__' THEN 1 else o.SPCODE end = 
CASE  WHEN  t.[FILENAME] = 'C_ETI_' THEN 1
	      WHEN  t.[FILENAME] = 'C_ETIL' THEN 1
	      WHEN  t.[FILENAME] = 'CTETI_' THEN 1
	      WHEN  t.[FILENAME] = 'C_PUP_' THEN 1
	      WHEN  t.[FILENAME] = 'C_PUPL' THEN 1
	      WHEN  t.[FILENAME] = 'CTPU__' THEN 1 else t.SPCODE end )
left join [Mort_Table] as m on( o.Sex = m.[Sex_Num] and o.AGE_AT_ENTRY+o.POLICY_YEAR = m.[Age_x]  )
left join (select * from Map_Data where Port_Type like'Trad%' and PLAN_SEQ = 1) as k on(o.POL_NUMBER = k.CERNO_POLNO and o.PACKAGE_CODE = k.PACKAGE_CODE)


go


-- Checking
-- old + new = 5902876
---- Here!!!!!!!!!!!!!!!!!!!!!!!!!!!

--====== Making Claim with Prophet_Result ======


create view Prophet_Mort_2 as

select c.CLAIM_TYPE,iif(c.CLAIM_TYPE is null,null,0.5) as [Exposure_Case_Claim],0.5*c.ACTUAL_CLM_AMT as [Exposure_SA_Claim],m.[q_x+0]*0.5 as [Ex_Case_Claim],m.[q_x+0]*c.ACTUAL_CLM_AMT/2 as [Ex_SA_Claim],
m.*,
c.CLAIM_STS_DESC,c.CLAIM_TYPE_CODE,c.PACKAGE_DESC,c.RIDER_CODE,c.RIDER_NAME,c.DOI_DATE,c.EFFECTIVE_DATE,c.FST_ISSUE_DATE,c.ISSUE_DATE,c.EXPIRE_DATE,c.DOC_RCV_DATE,c.NOTICE_DATE,c.CLM_APV_DATE,c.CLM_PAYMENT_DATE,c.CLAIM_INC_DATE,c.CAUSE_OF_DTH_CODE,c.DIAGNOSIS,c.CAUSE_DESC,c.CAUSE_OF_NUL_CODE,c.NULLIFY_DESC,c.IPD_OPD,c.HOSPITAL,c.ADMISSION_DATE,c.DISCHARGE_DATE,c.LEN_OF_STAY,c.CURR,c.TOTAL_CLM_AMT,c.ACTUAL_CLM_AMT,
c.POL_UNPAID_PREMIUM,c.POL_UNPAID_LOAN,c.POL_UNPAID_LOAN_INT,c.DEPOSIT_COND_BONUS,c.PAID_SUM_INSURED,c.CASH_VALUE,c.PREMIUM_PAID,c.CLM_PMT_MTD,c.CLM_PMT_MTD_DESC,c.BANK_CODE,c.ACCOUNT_NO,c.AGENT_CODE,c.SUM_INSURED as [Sum_Insured_Claim]

from Prophet_Mort as m left join Claim_Mort3 as c on( m.Reslt_Date ='Pre_result' and
iif(m.[FILENAME] in('C_ETI_','C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__') ,m.PACKAGE_CODE,m.[FILENAME]) = iif( m.[FILENAME] in('C_ETI_','C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__') ,c.M_PACKAGE_CODE,c.[FILENAME] )
and iif(m.[FILENAME] in('C_ETI_','C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__') ,'',m.SPCODE) =  iif(m.[FILENAME] in('C_ETI_','C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__') ,'',c.SPCODE )
and m.POL_NUMBER = c.M_Cerno_polno
and iif(m.PLAN_CODE_ETIRPU is not null,m.PLAN_CODE_ETIRPU,'') =iif(m.PLAN_CODE_ETIRPU is not null,c.[M_PLAN_COMPONENT],'') 

and iif(c.M_Port_Type ='GYRT',m.PACKAGE_CODE,'') = iif(c.M_Port_Type ='GYRT', c.orig_Pack_code   /*c.M_Master_Policy*/,'')
----and iif(c.M_Port_Type ='GYRT',m.DIST,'') = iif(c.M_Port_Type ='GYRT',c.[M_SUB_CHANNEL],'')

and iif(m.Product_PLAN ='G' and c.[PRODUCT_TYPE] ='GM',c.[ORIG_PACK_CODE],'') = iif(m.Product_PLAN ='G'  and c.[PRODUCT_TYPE] ='GM',m.Package_code,'')

---- Add Sub office for GYRT
AND iif( c.[PRODUCT_TYPE] ='GR',m.Product,'' ) = iif( c.[PRODUCT_TYPE] ='GR',cast(c.M_SUB_OFFICE as nvarchar) + cast(c.M_DEpendent_Code as nvarchar),'' )

and iif( c.[PRODUCT_TYPE] ='GR','',m.SEX) = iif( c.[PRODUCT_TYPE] ='GR','',c.CLAIM_GENDER) )



go



--create view Prophet_Mort_3 as

--select *
--from Prophet_Mort_2
--where claim_Type is not null

--go


--===================================================================================================================================================================================
--===================================================================================================================================================================================



--=============================================================================================================================================================
--  Result



-- Result!!!!!!!!!!!


----Create view Mortality_Result as

----select  Reslt_Date, [Product_Plan],Port,[Type],CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
----	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
----	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
----	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
----	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
----	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU' ELSE Product_Type end as [Product_Type], count(*) as [Num], sum(Exposure_Case) as [Exposure_Case], sum(Exposure_SA) as [Exposure_SA], sum(Ex_Case) as [Expected_Case], sum(Ex_SA) as [Expected_SA],
----Channel,[Sub_Channel Name],[Description],ENTRY_YEAR,Prem_Freq,iif(Sex = 0,'Male','Female') as [Sex],[Filename],AGE_AT_ENTRY,POL_TERM_Y,POLICY_YEAR,sum(ANNUAL_PREM) as [ANNUAL_PREM]
----,sum(SUM_ASSURED) as [SUM_ASSURED],sum(DEATH_BEN_PP) as [DEATH_BEN]
----from Prophet_Mort 
----where Product_Type not in('Rider - ACC','Rider - HI','Rider - HS','Rider - CI','Rider - RCC','Rider - TPD','Rider - WP')
------where    iif( Product ='IL' and [Type] ='Rider' and (Product_Type !='Rider - Term' or Product_Type != 'Rider PE'),'0','1' ) = '1'      ----  Cut OL rider
----group by CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
----	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
----	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
----	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
----	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
----	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU' ELSE Product_Type end,Reslt_Date , [Product_Plan],Port,[Type],Channel,[Sub_Channel Name],[Description],ENTRY_YEAR,Prem_Freq,Sex,[Filename],AGE_AT_ENTRY,POL_TERM_Y,POLICY_YEAR
----	 --,SUM_ASSURED,DEATH_BEN_PP
------order by Reslt_Date

----go



Create view _Mortality_Result as

select  Reslt_Date, [Product_Plan],Port,[Type],CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU' ELSE Product_Type end as [Product_Type], count(*) as [Num], sum(Exposure_Case) as [Exposure_Case], sum(Exposure_SA) as [Exposure_SA], sum(Ex_Case) as [Expected_Case], sum(Ex_SA) as [Expected_SA],
Channel,[Sub_Channel Name],[Description],ENTRY_YEAR,Prem_Freq,iif(Sex = 0,'Male','Female') as [Sex],[Filename],AGE_AT_ENTRY,POL_TERM_Y,POLICY_YEAR,sum(ANNUAL_PREM) as [ANNUAL_PREM]
,sum(SUM_ASSURED) as [SUM_ASSURED],sum(DEATH_BEN_PP) as [DEATH_BEN],sum([Exposure_Case_Claim]) as [Exposure_Case_Claim],sum([Exposure_SA_Claim]) as [Exposure_SA_Claim],sum(Ex_Case_Claim) as [Ex_Case_Claim],sum([Ex_SA_Claim]) as [Ex_SA_Claim],
sum(Exposure_Case)+sum([Exposure_Case_Claim]) as [Total_Exposure_Case],
sum(Exposure_SA) +sum([Exposure_SA_Claim]) as [Total_Exposure_SA],
sum(Ex_Case)+sum(Ex_Case_Claim) as [Total_Ex_Case],
sum(Ex_SA) +sum([Ex_SA_Claim]) as [Total_Ex_SA]
from Prophet_Mort_2 
where Product_Type not in('Rider - ACC','Rider - HI','Rider - HS','Rider - CI','Rider - RCC','Rider - TPD','Rider - WP')
--where    iif( Product ='IL' and [Type] ='Rider' and (Product_Type !='Rider - Term' or Product_Type != 'Rider PE'),'0','1' ) = '1'      ----  Cut OL rider
group by CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU' ELSE Product_Type end,Reslt_Date , [Product_Plan],Port,[Type],Channel,[Sub_Channel Name],[Description],ENTRY_YEAR,Prem_Freq,Sex,[Filename],AGE_AT_ENTRY,POL_TERM_Y,POLICY_YEAR
	 --,SUM_ASSURED,DEATH_BEN_PP
--order by Reslt_Date

go


---------------------------------- Making Table ----------------------------------

select * into Mortality_Result
from _Mortality_Result



























--============================================================== Back Up Old Result ===========================================================================
--=============================================================================================================================================================

---- Result First
----select Group_Type1,POLICY_YEAR,iif(Sex = 0,'M','F') as [Gender],count(*) as [Policy_No],sum(Exposure_Case) as [Sum_Expo_C],sum(Exposure_SA) as [Sum_Expo_SA],sum(Ex_Case) as [Sum_EX_C],sum(Ex_SA) as [Sum_EX_SA]
----from Prophet_Mort
----where [Type] ='Basic'
----group by Group_Type1,POLICY_YEAR,iif(Sex = 0,'M','F')
----order by Group_Type1,POLICY_YEAR,iif(Sex = 0,'M','F')



--select CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
--	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
--	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
--	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
--	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
--	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU'
--ELSE  [Group_Type1] END as [Group_Type1],POLICY_YEAR,iif(Sex = 0,'M','F') as [Gender],count(*) as [Policy_No],sum(Exposure_Case) as [Sum_Expo_C],sum(Exposure_SA) as [Sum_Expo_SA],sum(Ex_Case) as [Sum_EX_C],sum(Ex_SA) as [Sum_EX_SA]
--from Prophet_Mort
--where iif([Type] = 'RIDER' and Group_Type1 ='TS','Basic',[Type]) ='Basic'
--group by CASE WHEN  [FILENAME] = 'C_ETI_' THEN 'ETI'
--	 WHEN  [FILENAME] = 'C_ETIL' THEN 'ETI'
--	 WHEN  [FILENAME] = 'CTETI_' THEN 'ETI'
--	 WHEN  [FILENAME] = 'C_PUP_' THEN 'RPU'
--	 WHEN  [FILENAME] = 'C_PUPL' THEN 'RPU'
--	 WHEN  [FILENAME] = 'CTPU__' THEN 'RPU' ELSE  [Group_Type1] END  ,POLICY_YEAR,iif(Sex = 0,'M','F')




---- FOR ETI&RPU

--select t.[filename],t.Group_Type1,o.[PLAN_COMPONENT],o.[Sub_Channel],m.*
--from Prophet_Mort as m left join (select CERNO_POLNO,PLAN_COMPONENT,PACKAGE_CODE,sub_channel from Data_ETIRPU ) as o on(/*m.PACKAGE_CODE = o.PACKAGE_CODE and*/ m.POL_NUMBER = o.CERNO_POLNO )
--left join Prophet_Table as t on(o.PACKAGE_CODE = t.PACKAGE_CODE and t.PLAN_COMPONENT = o.plan_Component and t.SUB_CHANNEL = o.sub_Channel)
--where m.[FILENAME] In( 'C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__')
--and t.group_Type1 is null






------------------------------------------------------------------------------------------------------------
------- Checking ETI&RPU -----


--select t.[filename],t.Group_Type1,o.[PLAN_COMPONENT],o.[Sub_Channel],m.*
--from Prophet_Mort as m left join (select CERNO_POLNO,PLAN_COMPONENT,PACKAGE_CODE,sub_channel from Data_ETIRPU ) as o on(/*m.PACKAGE_CODE = o.PACKAGE_CODE and*/ m.POL_NUMBER = o.CERNO_POLNO )
--left join Prophet_Table as t on(o.PACKAGE_CODE = t.PACKAGE_CODE and t.PLAN_COMPONENT = o.plan_Component and t.SUB_CHANNEL = o.sub_Channel)
--where m.[FILENAME] In( 'C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__')
----and t.group_Type1 is null
--and t.Group_Type1 ='WL'





--select t.[filename],t.Group_Type1,o.[PLAN_COMPONENT],o.[Sub_Channel],m.*
--from Prophet_Mort as m left join (select CERNO_POLNO,PLAN_COMPONENT,PACKAGE_CODE,sub_channel from Data_ETIRPU ) as o on(/*m.PACKAGE_CODE = o.PACKAGE_CODE and*/ m.POL_NUMBER = o.CERNO_POLNO )
--left join Prophet_Table as t on(o.PACKAGE_CODE = t.PACKAGE_CODE and t.PLAN_COMPONENT = o.plan_Component and t.SUB_CHANNEL = o.sub_Channel)
--where m.[FILENAME] not In( 'C_ETIL','CTETI_','C_PUP_','C_PUPL','CTPU__')
----and t.group_Type1 is null
--and t.Group_Type1 ='WL'
--and o.PLAN_COMPONENT = 'WAT004'




----select filename,count(*) as [num]
----from  Prophet_Mort
----group by filename



----select filename,Reslt_Date,sum(Exposure_Case) as [Sum_Expo_C]
----from Prophet_Mort
----where [Type] ='Basic'
----group by FILENAME,Reslt_Date



---- Make the UL and ETI&RPU

----select o.*
----from Prophet_Mort as o left join (select * 
----from ACT_DATA_201809..TMP_ATF020PF 
----where CUR_POL_STS in('10','11','12','13')) as d on(o.POL_NUMBER = d.CERNO_POLNO and o.PACKAGE_CODE = d.PACKAGE_CODE )
----where o.[FILENAME] in('C_ETI_'
----,'C_ETIL'
----,'C_PUP_'
----,'C_PUPL'
----,'CTETI_'
----,'CTPU__')



-----Rename Prophet_Merge_All_201612_201712
