create or replace view KSFPA.ONLINE_UGAM_PVT.SLA_TARGET_SUPER(
	ACCOUNTING_YEAR,
	ACCOUNTING_MONTH_NUMBER,
	FINANCIAL_WEEK_IN_PERIOD,
	FY_PW,
	STORE_NO,
	STORE_NAME,
	STATE,
	STATE_ABV,
	CON_TOTAL_1DAY,
	CON_PASS_1DAY,
	CON_PASS_2DAY
) as

     WITH cte0 AS(
		SELECT  	
			ord.createdts ord_date,
			con.p_senttowarehousedate stw_date,
			con.p_packeddate con_packedTS,
			COALESCE(con.p_canceldate, con.p_packeddate) completed_date,
			ord.p_code ord_no,
			con.p_code con_no,
			pos.p_storenumber store_no,
			pos.p_name store_name,
			adrFrom.p_district from_state, 
			adrDest.p_district dest_state,
			adrDest.p_postalcode dest_postcode,
			adrDest.p_town dest_suburb,
			constatus.code con_status,
			CASE WHEN (DATEDIFF(MIN, con.p_senttowarehousedate, con.p_packeddate) / 
				 	  (1440*1.0)) - ph."lifetime_count_ph" <= 1 THEN 1 ELSE 0 END c0_a2,	
			CASE WHEN (DATEDIFF(MIN, con.p_senttowarehousedate, con.p_packeddate) / 
				 	  (1440*1.0)) - ph."lifetime_count_ph" <= 2 THEN 1 ELSE 0 END c0_b2,	
			ph."lifetime_count_ph",	  
			(DATEDIFF(MIN, con.p_senttowarehousedate, con.p_packeddate) / 
			(1440*1.0)) - ph."lifetime_count_ph" fulfil_duration_elapsed, --VAL ONLY
			(DATEDIFF(MIN, con.p_senttowarehousedate, CONVERT_TIMEZONE('Australia/Sydney', getdate())) / --aest
			(1440*1.0)) - ph."lifetime_count_ph"  as current_age_days
				
		FROM TSFPA.HYBRIS.VH_CONSIGNMENTS con 
		RIGHT JOIN KSFPA.ONLINE_UGAM_PVT.ldb_con_ph_count_01_24_hr_sla_target ph ON (ph.con_no = con.p_code)
		LEFT JOIN TSFPA.HYBRIS.VH_ORDERS_ALL ord ON (con.p_order = ord.PK)
		LEFT JOIN TSFPA.HYBRIS.VC_WAREHOUSES wh ON (wh.pk = con.p_warehouse)
		LEFT JOIN TSFPA.HYBRIS.VC_POS2WAREHOUSEREL pwr ON (pwr.TargetPK = wh.PK)
		LEFT JOIN TSFPA.HYBRIS.VC_POINTOFSERVICE pos  ON (pos.pk = pwr.SourcePK)
		LEFT JOIN TSFPA.HYBRIS.VH_ADDRESS_ALL adrFrom ON (pos.p_address = adrFrom.PK)
		LEFT JOIN TSFPA.HYBRIS.VH_ADDRESS_ALL adrDest ON (con.p_shippingaddress = adrDest.PK)
		LEFT JOIN TSFPA.HYBRIS.ENUMERATIONVALUES conStatus ON (con.p_status = conStatus.PK)
		LEFT JOIN TSFPA.HYBRIS.ENUMERATIONVALUES ofcType ON (con.p_ofcorderdeliverytype = ofcType.PK)
	 
	    WHERE ofctype.code <> 'INSTORE_PICKUP' 		
	    AND con.p_canceldate IS NULL
	    AND constatus.code NOT IN ('SENT_TO_WAREHOUSE', 'WAVED')
        AND con.CIR_ISCURRENT=TRUE AND con.CIR_ISDELETED=FALSE
        AND ORD.CIR_ISCURRENT=TRUE AND ORD.CIR_ISDELETED=FALSE
        AND wh.CIR_ISCURRENT=TRUE AND wh.CIR_ISDELETED=FALSE
        AND pwr.CIR_ISCURRENT=TRUE AND pwr.CIR_ISDELETED=FALSE
        AND pos.CIR_ISCURRENT=TRUE AND pos.CIR_ISDELETED=FALSE
        AND adrFrom.CIR_ISCURRENT=TRUE AND adrFrom.CIR_ISDELETED=FALSE
        AND adrDest.CIR_ISCURRENT=TRUE AND adrDest.CIR_ISDELETED=FALSE
	)
    
    SELECT
     cal.ACCOUNTING_YEAR,
     cal.ACCOUNTING_MONTH_NUMBER,
     cal.FINANCIAL_WEEK_IN_PERIOD,
	CONCAT('FY', cal.ACCOUNTING_YEAR,'P',LPAD(cal.ACCOUNTING_MONTH_NUMBER,2,'0'),'W',cal.FINANCIAL_WEEK_IN_PERIOD) AS FY_PW,
		cte0.store_no, --ppd.store_id,
		cte0.store_name, --ppd.location_name,
        ppd.STATE_NAME AS STATE,
        ppd.STORE_LOCATED_STATE_CODE AS STATE_ABV,
	 	COUNT(DISTINCT(con_no)) con_total_1day,
	 	SUM(c0_a2) con_pass_1day,
	 	SUM(c0_b2) con_pass_2day
	 	
		 FROM cte0 
		 LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE cal 
         ON (cal.DATE = cte0.completed_date::DATE)
		 LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION  ppd 
         ON (ppd.location_code = cte0.store_no)  
         
		 GROUP BY 1,2,3,4,5,6,7,8;