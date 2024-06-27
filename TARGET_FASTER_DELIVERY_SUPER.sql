create or replace view KSFPA.ONLINE_UGAM_PVT.TARGET_FASTER_DELIVERY_SUPER(
	FINANCIAL_YEAR,
	FY_PW,
	FYP_RANK,
	STORE_NO,
	STORE_NAME,
	ORD_NO,
	CON_NO,
	CARRIER,
	STATE,
	STW_DATE,
	PACKED_DATE,
	PROMISE_DATE,
	CON_DELIVERY_COST,
	OFC_TYPE,
	CON_COUNT,
	WK_P_QTY,
	WK_S_QTY,
	STW_TZ_ADJ,
	PACKED_TZ_ADJ,
	DAY_OF_WK,
	TZ_ADJ_DUE_TS,
	C_FAILED_FULFILMENT_SLA
) as 


 SELECT 
	cal.FINANCIAL_YEAR,
    CONCAT('FY',        
    cal.ACCOUNTING_YEAR,'P',LPAD(cal.ACCOUNTING_MONTH_NUMBER,2,'0'),'W',cal.FINANCIAL_WEEK_IN_PERIOD) AS FY_PW,
    DENSE_RANK() OVER(ORDER BY FY_PW DESC) AS FYP_RANK,
	ppd.LOCATION_CODE AS store_no,
	ppd.LOCATION_NAME AS store_name,
	ord.p_code AS ord_no,
	con.p_code AS con_no, 			--VAL ONLY
	con.p_carrier AS carrier,
	ppd.store_located_state_code AS state,
	con.p_senttowarehousedate AS stw_date,
	con.p_packeddate AS packed_date,
	con.p_consignmentpromisedate AS promise_date,
	con.p_shippitprice AS con_delivery_cost,
	ofctype.code AS ofc_type,
    
CASE 
    WHEN ofctype.code =  'INSTORE_PICKUP' AND constatus.code <> 'CANCELLED' THEN COUNT(DISTINCT(con.p_code))
	WHEN ofctype.code <> 'INSTORE_PICKUP' AND constatus.code NOT IN ('CANCELLED', 'PICKED') THEN       
    COUNT(DISTINCT(con.p_code))
	ELSE 0 END AS con_count,
CASE 
    WHEN ofctype.code = 'INSTORE_PICKUP' THEN SUM(coen.p_quantity)
	WHEN ofctype.code <> 'INSTORE_PICKUP' 
	AND constatus.code NOT IN ('PICKED') THEN SUM(coen.p_quantity) ELSE 0 END AS wk_p_qty,		 
CASE 
    WHEN ofctype.code = 'INSTORE_PICKUP' THEN SUM(coen.p_shippedQuantity)
	WHEN ofctype.code <> 'INSTORE_PICKUP' 
	AND constatus.code NOT IN ('CANCELLED', 'PICKED') THEN SUM(coen.p_shippedQuantity) ELSE 0 END AS wk_s_qty,		 
	CONVERT_TIMEZONE('Australia/Melbourne', ppd.location_timezone, con.p_senttowarehousedate) AS stw_tz_adj,
	CONVERT_TIMEZONE('Australia/Melbourne', ppd.location_timezone, con.p_packeddate) AS packed_tz_adj,
	DATE_PART(dayofweek, stw_tz_adj) AS day_of_wk,
    
CASE 
    WHEN day_of_wk = 0 THEN (stw_tz_adj::DATE + 1 || ' 15:00:00')::TIMESTAMP
	WHEN day_of_wk = 6 THEN (stw_tz_adj::DATE + 2 || ' 15:00:00')::TIMESTAMP
	WHEN day_of_wk IN (1, 2, 3, 4) AND DATE_PART(HOUR, stw_tz_adj) < 12 THEN 
		 (stw_tz_adj::DATE || ' 15:00:00')::TIMESTAMP
	WHEN day_of_wk IN (1, 2, 3, 4) AND DATE_PART(HOUR, stw_tz_adj) >= 12 THEN 
		 (stw_tz_adj::DATE + 1 || ' 15:00:00')::TIMESTAMP
	WHEN day_of_wk = 5 AND DATE_PART(HOUR, stw_tz_adj) < 12 THEN 
		 (stw_tz_adj::DATE || ' 15:00:00')::TIMESTAMP
	WHEN day_of_wk = 5 AND DATE_PART(HOUR, stw_tz_adj) >= 12 THEN 
		 (stw_tz_adj::DATE + 3 || ' 15:00:00')::TIMESTAMP		 	  
	ELSE NULL END AS tz_adj_due_ts,
CASE 
    WHEN con.p_canceldate IS NULL AND packed_tz_adj > tz_adj_due_ts THEN 1
	WHEN con.p_canceldate IS NOT NULL AND 
	CONVERT_TIMEZONE('Australia/Melbourne', ppd.location_timezone, con.p_canceldate) > tz_adj_due_ts
	THEN 1 ELSE 0 END AS c_failed_fulfilment_sla --Cleo wants to report ON the accuracy AND NOT the error
	
 FROM 
    TSFPA.HYBRIS.VH_CONSIGNMENTS con 
	LEFT JOIN TSFPA.HYBRIS.VH_CONSIGNMENTENTRIES coen  ON coen.p_consignment = con.pk
	LEFT JOIN TSFPA.HYBRIS.VH_ORDERS_ALL ord ON con.p_order = ord.PK
	LEFT JOIN TSFPA.HYBRIS.VC_WAREHOUSES AS wh on wh.pk = con.p_warehouse
	LEFT JOIN TSFPA.HYBRIS.VC_POS2WAREHOUSEREL AS pwr ON  pwr.TargetPK = wh.PK
	LEFT JOIN TSFPA.HYBRIS.VC_POINTOFSERVICE AS pos  ON pos.pk = pwr.SourcePK
	--LEFT JOIN production.addresses "adrDest" ON (con.p_shippingaddress = adrDest.PK)
	LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES conStatus ON con.p_status = conStatus.PK
	LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES AS ofcType ON con.p_ofcorderdeliverytype = ofcType.PK
	LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE cal 
    ON (cal.date = COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate)::DATE)
    LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION AS PPD
    ON pos.p_storenumber = PPD.LOCATION_CODE
		
 WHERE 
    --COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate)::DATE >=            
    --CONVERT_TIMEZONE('Australia/Melbourne',getdate())::DATE - INTERVAL '10 day'
    COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate)::DATE >= '2023-06-01'
    AND ord.p_fraudulent <> 1
    AND constatus.code NOT IN ('SENT_TO_WAREHOUSE', 'WAVED')
    AND pos.p_storenumber IS NOT NULL
    AND ORD.CIR_ISCURRENT=TRUE AND ORD.CIR_ISDELETED=FALSE
    AND CON.CIR_ISCURRENT=TRUE AND CON.CIR_ISDELETED=FALSE
    AND WH.CIR_ISCURRENT=TRUE AND WH.CIR_ISDELETED=FALSE
    AND PWR.CIR_ISCURRENT=TRUE AND PWR.CIR_ISDELETED=FALSE
    AND POS.CIR_ISCURRENT=TRUE AND POS.CIR_ISDELETED=FALSE
    AND conStatus.CIR_ISCURRENT=TRUE AND conStatus.CIR_ISDELETED=FALSE
    AND COEN.CIR_ISCURRENT=TRUE AND COEN.CIR_ISDELETED=FALSE
 
 GROUP BY 	
    cal.FINANCIAL_YEAR, ppd.LOCATION_CODE, FY_PW, ppd.LOCATION_NAME, ord.p_code, con.p_code, con.p_carrier,         ppd.store_located_state_code,con.p_senttowarehousedate, con.p_packeddate, con.p_consignmentpromisedate,         con.p_shippitprice, ofctype.code,stw_tz_adj, packed_tz_adj, day_of_wk, tz_adj_due_ts,               
    c_failed_fulfilment_sla, constatus.code;