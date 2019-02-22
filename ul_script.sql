---- Create user defined functions

--Create function dbo.[MakeISODate] 
--	(@inputdate numeric) 
--	RETURNS date
--	BEGIN
--		Declare @returndate date;

--		Set @returndate = convert(date, convert(nvarchar, convert(numeric, @inputdate)))

--		Return @returndate
--	END;

---- Create study date endpoints
--select convert(date, '20171231', 112) [BeginDate]
--	, convert(date, '20181231', 112) [EndDate]
--into #StudyDates

---- select * from #StudyDate_Endpoints


--------------------------------------
-- UL mortality table thing query
--------------------------------------

-- GL Data Scrub 
/*

select N'DEATH'			[CLAIM_TYPE]
	, (TRAN_YY*10000) + (TRAN_MM*100) + 1.0 [CLAIM_INC_DATE]
	, sum(CASE
		WHEN POLICY_CERTIFICATE in (20002614,20003040,20004167) and left(prophet_code, 3) like 'U_B' then 0
		else amount
		end)			[TOTAL_CLM_AMT]
	, sum(CASE
		WHEN POLICY_CERTIFICATE in (20002614,20003040,20004167) and left(prophet_code, 3) like 'U_B' then 0
		else amount
		end)			[ACTUAL_CLM_AMT]
	, TRAN_YY
	, TRAN_MM
	, SYS_TYPE
	, POLICY_CERTIFICATE
	, PLAN_CODE
	, ACCT_NO
	, ACCT_NAME
	, Prophet_code
	, sum(CASE
		WHEN POLICY_CERTIFICATE in (20002614,20003040,20004167) and left(prophet_code, 3) like 'U_B' then 0
		else amount
		end)  [Amount]
into GL_Data_UL_mod
from GL_Data_UL
group by TRAN_YY, TRAN_MM, SYS_TYPE, POLICY_CERTIFICATE, PLAN_CODE, ACCT_NO, ACCT_NAME, Prophet_code
having sum(CASE
		WHEN POLICY_CERTIFICATE in (20002614,20003040,20004167) and left(prophet_code, 3) like 'U_B' then 0
		else amount
		end) > 0
order by POLICY_CERTIFICATE, Prophet_code
*/

use Mortality_TEST

declare @nb_issyear int
set @nb_issyear = 2018

select N'PLT'				[OWNER]
	, N'UL'					[PRODUCT_TYPE_N]
	, N'DEC18'				[MONTH_T]
	, N'DEC17'				[MONTH_T-1]
	, CA2.[CASE_T]
	, CA3.[CASE_T-1]
	, CA4.[DEATH_CASE_T]
	, CA5.[ATTAINED_AGE_T]
	, CA6.[ATTAINED_AGE_T-1]
	, CA7.[EXPOSURE_CASE]
	, CA8.[EXPOSURE_SA]
	, CA9.[EXPECTED_CASE]
	, CA10.[EXPECTED_SA]
	, q1.[q_x+0]			[Q_T]
	, q0.[q_x+0]			[Q_T-1]
	, N'Non-GIO (End, Term, Par and UL)'		[PROPHET_GROUP]
	, ca11.[Death_Type]		[DEATH_TYPE]
	, ca12.[Death_Amt]		[DEATH_AMT]
	--, C.CLAIM_TYPE			[CLAIM_TYPE]
	--, C.CLAIM_INC_DATE		[CLAIM_INC_DATE]
	--, C.TOTAL_CLM_AMT		[TOTAL_CLM_AMT]
	--, C.ACTUAL_CLM_AMT		[ACTUAL_CLM_AMT]
	--, (case 	--	when claim_type is null then NULL
	--	when claim_type = 'death' then CASE
	--		WHEN (C.TRAN_YY - 543) > mp.[entry_year] THEN CASE 
	--			WHEN (C.TRAN_MM*100 + 1) >= (MP.[ENTRY_MONTH]*100 + MP.[ENTRY_DAY]) THEN (C.TRAN_YY - 543) - MP.[ENTRY_YEAR] + 1
	--			ELSE (C.TRAN_YY - 543) - MP.[ENTRY_YEAR]
	--			END
	--		ELSE 1
	--		END
	--	END)				[POLICY_YEAR_CLM]
	--, ca1.POLICY_YEAR_CLM	[POLICY_YEAR_CLM]
	, C.CLAIM_INC_DATE		[NOTICE_DATE]
	, C.CLAIM_INC_DATE		[CLAIM_INC_DATE]
	, NULL					[CAUSE_OF_DTH_CODE]
	, N'PAID'				[CLAIM_STS_DESC]
	, p8.DEATH_BEN_PP		[DEATH_BEN_T]
	, P8.SUM_ASSURED		[SA_T]
	, P8.POL_NUMBER			[POLICYNO_T]
	, P8.[FILENAME]			[FILENAME_T]
	, P8.POLICY_YEAR		[POLICY_YEAR_T]
	, p7.DEATH_BEN_PP		[DEATH_BEN_T-1]
	, p7.SUM_ASSURED		[SA_T-1]
	, p7.POL_NUMBER			[POLICYNO_T-1]
	, p7.[FILENAME]			[FILENAME_T-1]
	, p7.POLICY_YEAR		[POLICY_YEAR_T-1]
	, mp.*
	, NULL	[ZIP_CODE]
	, NULL	[ADDR_1]
	, NULL	[ADDR_2]
	, NULL	[ADDR_3]
	, NULL	[OCCU_CLASS]
	, NULL	[MARITAL_STS]
	, NULL	[DATE_OF_BIRTH]
	, NULL	[AGENCY_CODE]
	, NULL	[CHANNEL]
	, NULL	[SUB_CHANNEL]
	, NULL	[OCCU_CODE]
	, NULL	[MEDICAL_FLAG]
--into Mortality_UL_s1_v1
from prophet_ul_ifandnonif mp
left join (select * from Prophet_Result_201812_Map where block = 'ul') p8 on 
	p8.[PRODUCT] = mp.[PRODUCT]
	and p8.[PACKAGE_CODE] = mp.[PACKAGE_CODE]
	and p8.[POL_NUMBER] = mp.[POLNO]
	and p8.[FILENAME] = mp.[FILENAME]
	and p8.[ENTRY_YEAR] = mp.[ENTRY_YEAR]
	and p8.[ENTRY_MONTH] = mp.[ENTRY_MONTH]
	and (case 
		when p8.[POL_NUMBER] = 20001850 then case
			when p8.MODEL_POINT = 5 then 10
			when p8.MODEL_POINT = 6 then 21
			else '' end
		when p8.[POL_NUMBER] = 20001919 then case
			when p8.MODEL_POINT = 8 then 4
			when p8.MODEL_POINT = 9 then 5
			else '' end
		else '' end)
		=
		case
		when p8.[POL_NUMBER] = 20001850 and p8.MODEL_POINT in (5, 6) then mp.[ENTRY_DAY]
		when p8.[POL_NUMBER] = 20001919 and p8.MODEL_POINT in (8, 9) then mp.[ENTRY_DAY]
		else '' end
left join (select * from Prophet_Result_201712_Map where block='ul') p7 on
	p7.[PRODUCT] = mp.[PRODUCT]
	and p7.[PACKAGE_CODE] = mp.[PACKAGE_CODE]
	and p7.[POL_NUMBER] = mp.[POLNO]
	and p7.[FILENAME] = mp.[FILENAME]
	and p7.[ENTRY_YEAR] = mp.[ENTRY_YEAR]
	and p7.[ENTRY_MONTH] = mp.[ENTRY_MONTH]
	and (case 
		when p7.[POL_NUMBER] = 20001850 then case
			when p7.MODEL_POINT = 5 then 10
			when p7.MODEL_POINT = 6 then 21
			else '' end
		when p7.[POL_NUMBER] = 20001919 then case
			when p7.MODEL_POINT = 8 then 4
			when p7.MODEL_POINT = 9 then 5
			else '' end
		else '' end)
		=
		case
		when p7.[POL_NUMBER] = 20001850 and p7.MODEL_POINT in (5, 6) then mp.[ENTRY_DAY]
		when p7.[POL_NUMBER] = 20001919 and p7.MODEL_POINT in (8, 9) then mp.[ENTRY_DAY]
		else '' end
left join GL_Data_UL_mod c on
	c.[policy_certificate] = mp.[polno]
	and c.[plan_code] = mp.[package_code]
	and c.[prophet_code] = mp.[filename]
CROSS APPLY (SELECT CASE
	WHEN C.CLAIM_TYPE IS NULL THEN NULL
	WHEN (C.CLAIM_TYPE IS NOT NULL) AND MP.ISSUE_YEAR = @nb_issyear THEN 'Death_NB'
	ELSE 'Death'
	END)		ca11([Death_Type])
CROSS APPLY (SELECT CASE
	WHEN CA11.Death_Type = 'Death_NB' then mp.[SUM_ASSURED]
	WHEN CA11.Death_Type = 'Death' then p7.DEATH_BEN_PP
	ELSE NULL
	END)		ca12([Death_Amt]) -- Amount awarded for death claims
cross apply (select (case 
		when C.CLAIM_TYPE is null then NULL
		when c.claim_type LIKE 'death%' then CASE
			WHEN (C.TRAN_YY - 543) > mp.[entry_year] THEN CASE 
				WHEN (C.TRAN_MM*100 + 1) >= (MP.[ENTRY_MONTH]*100 + MP.[ENTRY_DAY]) THEN (C.TRAN_YY - 543) - MP.[ENTRY_YEAR] + 1
				ELSE (C.TRAN_YY - 543) - MP.[ENTRY_YEAR]
				END
			ELSE 1
			END
		END)) ca1(POLICY_YEAR_CLM)
CROSS APPLY (SELECT CASE WHEN P8.[POL_NUMBER] IS NULL THEN 0 ELSE 1 END) CA2(CASE_T) -- Indicates for IF status at study endpoints or death during study window
CROSS APPLY (SELECT CASE WHEN P7.[POL_NUMBER] IS NULL THEN 0 ELSE 1 END) CA3([CASE_T-1])
CROSS APPLY (SELECT CASE WHEN C.[CLAIM_TYPE] IS NULL THEN 0 ELSE 1 END) CA4(DEATH_CASE_T)
CROSS APPLY (SELECT CASE 
	WHEN CA2.[CASE_T] IS NULL AND CA4.[DEATH_CASE_T] IS NULL THEN NULL -- Covers lapses
	ELSE MP.[AGE_AT_ENTRY] + CASE
		WHEN CA11.[Death_Type] = 'DEATH_NB' THEN 0 
		WHEN CA11.[Death_Type] = 'DEATH' THEN NULL
--		WHEN CA3.[CASE_T-1] = 0 AND CA2.[CASE_T] = 1 THEN 0 -- tHIS SHOULD BE ADDED FOR NB POLICIES
		ELSE P8.POLICY_YEAR END
	END)						CA5([ATTAINED_AGE_T])
--CROSS APPLY (SELECT mp.[AGE_AT_ENTRY] + COALESCE(CASE 
--	WHEN C.CLAIM_TYPE IS NOT NULL THEN p7.POLICY_YEAR 
--	ELSE NULL 
--	END, p8.[POLICY_YEAR], 0))			CA5([ATTAINED_AGE_T])  

CROSS APPLY (SELECT CASE WHEN CA3.[CASE_T-1] = 0 THEN NULL
	ELSE MP.[AGE_AT_ENTRY] + P7.[POLICY_YEAR] END)		CA6([ATTAINED_AGE_T-1])
LEFT JOIN mort_table q0 on -- for calculating Qx at T-1
	q0.[Sex_Num] = mp.[sex]
	and q0.[age_x] = CA6.[ATTAINED_AGE_T-1]
LEFT JOIN Mort_Table q1 on -- for calculating Qx at T
	q1.[Sex_Num] = mp.[SEX]
	and q1.[Age_x] = CA5.[ATTAINED_AGE_T]
-- CROSS APPLY (SELECT mp.[AGE_AT_ENTRY] + COALESCE(P7.POLICY_YEAR, 0)) CA6([ATTAINED_AGE_T-1])
CROSS APPLY (SELECT (CA2.[CASE_T] + CA3.[CASE_T-1] + CA4.[DEATH_CASE_T])/2.0) CA7([EXPOSURE_CASE])
CROSS APPLY (SELECT (CA2.[CASE_T] * ISNULL(P8.[DEATH_BEN_PP], 0) + CA3.[CASE_T-1] * ISNULL(P7.[DEATH_BEN_PP], 0) + CA4.[DEATH_CASE_T] * ISNULL(C.[TOTAL_CLM_AMT], 0))/2.0) CA8([EXPOSURE_SA])
CROSS APPLY (SELECT ((CA2.[CASE_T] + CA4.[DEATH_CASE_T])*(ISNULL(q1.[q_x+0], 0)) + (CA3.[CASE_T-1]*(ISNULL(q0.[q_x+0], 0))))/2.0) CA9([EXPECTED_CASE])
CROSS APPLY (SELECT ((((CA2.[CASE_T] * ISNULL(P8.[DEATH_BEN_PP], 0)) + (CA4.[DEATH_CASE_T] * ISNULL(P7.[DEATH_BEN_PP], 0)))*(ISNULL(Q1.[q_x+0], 0)) 
	+ (ca3.[CASE_T-1] * ISNULL(p7.[DEATH_BEN_PP], 0))*(ISNULL(q0.[q_x+0], 0)))/2.0)) ca10([EXPECTED_SA])

WHERE (ca11.[Death_Type] IS NOT NULL OR P8.[POL_NUMBER] IS NOT NULL OR P7.[POL_NUMBER] IS NOT NULL) -- Filter out non-exposed policies
	
-- where ( m.[Death_Type] is not null or m.POLICYNO_T is not null or m.[POLICYNO_T-1] is not null)

--select * from prophet_ul_ifandnonif where polno = 20001850
--select * from Prophet_Result_201812_Map where POL_NUMBER in (20001850, 20001919) and BLOCK = 'ul'
--select * from [ca_database_201812].dbo.[struc_ul] where polno = 20001850

-- 11022
-- 11022
-- Test 
/*
select * from [ca_database_201812].dbo.[struc_ul] where ppstat = 'if' -- 7915 rows

select * from Prophet_Result_201812_Map where block = 'ul' -- 7913 rows
select * from Prophet_Result_201812_Map where [data] = 'ul'  --  7913 rows
select * from Prophet_Result_201812_Map where Product_Type = 'ul' -- 7913 rows
select * from Prophet_Result_201812_Map where [filename] like 'u_%' -- 7892 rows */

--use CA_Database_201812
--select count(*) from struc_UL
