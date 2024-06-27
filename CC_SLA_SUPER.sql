create or replace view KSFPA.ONLINE_UGAM_PVT.CC_SLA_SUPER(
	FY_PW,
	STW_DATE,
	TOTAL_CON_COUNT,
	STATE_NAME,
	LOCATION_CODE,
	LOCATION_NAME,
	SLA_ACHIEVED_COUNT
) as 

(    
    WITH cte0 AS(
		SELECT 
            CONCAT('FY', DD.ACCOUNTING_YEAR,'P',LPAD(DD.ACCOUNTING_MONTH_NUMBER,2,'0'),'W',DD.FINANCIAL_WEEK_IN_PERIOD) AS FY_PW,
			ord.p_code AS ord_no,
			con.p_code AS con_no,
			con.p_senttowarehousedate AS stw_date,
			con.p_readyForPickupDate AS ready_for_pick_up_date,
			pos.p_storenumber AS store_no,
			con.p_consignmentpromisedate AS promise_date,
			CASE WHEN con.p_readyForPickupDate IS NOT NULL THEN 1
				 WHEN CURRENT_TIMESTAMP > con.p_consignmentpromisedate THEN 1
				 ELSE 0 END AS c_sla_complete,
			DATEDIFF(MIN, con.p_senttowarehousedate, con.p_consignmentpromisedate) AS possible_sla_min 
			      
		FROM 
        TSFPA.HYBRIS.VH_CONSIGNMENTS con 
        LEFT JOIN TSFPA.HYBRIS.VH_ORDERS_ALL ord ON con.p_order = ord.PK
        LEFT JOIN TSFPA.HYBRIS.VC_WAREHOUSES wh on wh.pk = con.p_warehouse
		LEFT JOIN TSFPA.HYBRIS.VC_POS2WAREHOUSEREL pwr ON  pwr.TargetPK = wh.PK
		LEFT JOIN TSFPA.HYBRIS.VC_POINTOFSERVICE pos  ON pos.pk = pwr.SourcePK
		LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES ofcType ON con.p_ofcorderdeliverytype = ofcType.PK
        LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE AS DD 
        ON DATE(con.p_packeddate) = DD.DATE 
				
		WHERE con.p_senttowarehousedate >= '2023-01-01'
        --AND con.p_senttowarehousedate <= '2024-04-27'
		AND ord.p_fraudulent <> 1
		AND con.p_canceldate IS NULL
		AND con.p_consignmentpromisedate IS NOT NULL
		AND OFCTYPE.code = 'INSTORE_PICKUP'
        AND ORD.CIR_ISCURRENT=TRUE AND ORD.CIR_ISDELETED=FALSE
        AND CON.CIR_ISCURRENT=TRUE AND CON.CIR_ISDELETED=FALSE
        AND WH.CIR_ISCURRENT=TRUE AND WH.CIR_ISDELETED=FALSE
        AND PWR.CIR_ISCURRENT=TRUE AND PWR.CIR_ISDELETED=FALSE
        AND POS.CIR_ISCURRENT=TRUE AND POS.CIR_ISDELETED=FALSE
        AND ofcType.CIR_ISCURRENT=TRUE AND ofcType.CIR_ISDELETED=FALSE
		GROUP BY FY_PW,ord.p_code,con.p_code, con.p_senttowarehousedate, con.p_readyForPickupDate, pos.p_storenumber, 
				 con.p_consignmentpromisedate
	) 
	SELECT 
	 	CTE0.FY_PW AS FY_PW,
        cte0.stw_date::DATE AS stw_date,
	 	COUNT(cte0.con_no) AS total_con_count,
	 	ppd.STATE_NAME,
	 	ppd.LOCATION_CODE,
	 	ppd.LOCATION_NAME,
	 	SUM(CASE WHEN cte0.ready_for_pick_up_date <= cte0.promise_date THEN 1 ELSE 0 END) AS sla_achieved_count  			
	FROM cte0
	LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION ppd 
    ON ppd.location_code = cte0.store_no
	WHERE ppd.STATE_NAME IS NOT NULL
	AND cte0.possible_sla_min >= 100 
	AND cte0.c_sla_complete = 1
	GROUP BY 	FY_PW,cte0.stw_date::DATE, ppd.STATE_NAME, ppd.LOCATION_CODE, ppd.LOCATION_NAME 
	ORDER BY 	cte0.stw_date::DATE
);