-- REQUIREMENTS:
-- Import and update table [Val_Date]



-- Transforming "Death Benefit Factor Depending on Age" table into a relational form via UNPIVOT
-- Insert query here

SELECT [PRODUCT NAME],[age],[db_factor] into [DTH_FAC_AGE_TAB]
FROM(
	SELECT [PRODUCT NAME]
		,[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63],[64],[65],[66],[67],[68],[69],[70],[71],[72],[73],[74],[75],[76],[77],[78],[79],[80],[81],[82],[83],[84],[85],[86],[87],[88],[89],[90],[91],[92],[93],[94],[95],[96],[97],[98],[99]
		FROM [PLT_DTH_FAC_AGE]) AS P
UNPIVOT
	([DB_FACTOR] FOR [AGE] IN
		([0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63],[64],[65],[66],[67],[68],[69],[70],[71],[72],[73],[74],[75],[76],[77],[78],[79],[80],[81],[82],[83],[84],[85],[86],[87],[88],[89],[90],[91],[92],[93],[94],[95],[96],[97],[98],[99])
	) AS UNPIV
-- Transforming "Death Benefit Factor Depending on Policy Year" table into a relational form via UNPIVOT
-- Insert query here
SELECT [PRODUCT NAME],[POL_Y],[db_factor] into [DTH_FAC_POL_YR_TAB]
FROM(
	SELECT [PRODUCT NAME]
		,[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63],[64],[65],[66],[67],[68],[69],[70],[71],[72],[73],[74],[75],[76],[77],[78],[79],[80],[81],[82],[83],[84],[85],[86],[87],[88],[89],[90],[91],[92],[93],[94],[95],[96],[97],[98],[99]
		FROM [PLT_DTH_FAC_POL_YR]) AS P
UNPIVOT
	([DB_FACTOR] FOR [POL_Y] IN
		([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63],[64],[65],[66],[67],[68],[69],[70],[71],[72],[73],[74],[75],[76],[77],[78],[79],[80],[81],[82],[83],[84],[85],[86],[87],[88],[89],[90],[91],[92],[93],[94],[95],[96],[97],[98],[99])
	) AS UNPIV


-- Death Benefit Factor 
-- Insert query here

-- Set valuation month info
DECLARE @VALDATE DATE
SET @VALDATE = (SELECT Beg_Date_E FROM Val_Date)


SELECT top 10 P.[FILENAME]
	, P.[AGE_AT_ENTRY]
	, P.[ENTRY_MONTH]
	, P.[ENTRY_YEAR]
	, DATEDIFF(MM, CONVERT(DATE, P.[ENTRY_YEAR] + '-' + [ENTRY_MONTH] + '-' + [ENTRY_DAY]), @VALDATE) 
		AS [T]
	, CAST((DATEDIFF(MM, CONVERT(DATE, [ENTRY_YEAR] + '-' + [ENTRY_MONTH] + '-' + [ENTRY_DAY]), @VALDATE)+11)/12 AS INT)
		AS [POLICY_YEAR]
	, P.[AGE_AT_ENTRY] + (CAST((DATEDIFF(MM, CONVERT(DATE, [ENTRY_YEAR] + '-' + [ENTRY_MONTH] + '-' + [ENTRY_DAY]), @VALDATE) +11) / 12 AS INT)) - 1		
		AS [AGE]
	--, T1.[DEATH_IND]
	--, T1.[AGE_SWITCH_DTH]
	--, T2.[db_factor]	AS [DBFAC_POLY]
	--, T3.[db_factor]	AS [DBFAC_AGE]
FROM [ZZ_PROPHET_ALL_IF_SAMPLE] AS P
LEFT JOIN [PARAM_PLT_201811] T1 ON
	P.[FILENAME] = T1.[Product Name]
LEFT JOIN [DTH_FAC_POL_YR_TAB] T2 ON
	P.[FILENAME] = T2.[PRODUCT NAME]
	AND CAST((DATEDIFF(MM, CONVERT(DATE, P.[ENTRY_YEAR] + '-' + P.[ENTRY_MONTH] + '-' + P.[ENTRY_DAY]), @VALDATE)+11)/12 AS INT) = T2.[POL_Y]
LEFT JOIN [DTH_FAC_AGE_TAB] T3 ON
	P.[FILENAME] = T3.[PRODUCT NAME]
	AND CAST((DATEDIFF(MM, CONVERT(DATE, P.[ENTRY_YEAR] + '-' + P.[ENTRY_MONTH] + '-' + P.[ENTRY_DAY]), @VALDATE)+11)/12 AS INT) = T3.[age]
