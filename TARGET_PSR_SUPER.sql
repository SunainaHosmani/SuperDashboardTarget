create or replace view KSFPA.ONLINE_UGAM_PVT.TARGET_PSR_SUPER(
	FY_PW,
	FK_CREATED_DATE,
	STATE,
	STATE_ABV,
	STORE_NO,
	OFC_TYPE,
	DELIVERY_MODE,
	CONSIGNMENTS,
	UNITS_ASSIGNED,
	UNITS_COMPLETED,
	WK_NET_SALES
) as 
    
    SELECT
    FY_PW,
    DATE(FK_CREATED_DATE) AS FK_CREATED_DATE,
    STATE,
    STATE_ABV,
    store_no,
    ofc_type,
    delivery_mode,
    SUM(CON_COUNT) AS CONSIGNMENTS,
    SUM(WK_P_QTY) AS UNITS_ASSIGNED,
    SUM(WK_S_QTY) AS UNITS_COMPLETED,
    SUM(WK_NET_SALES) AS WK_NET_SALES,
    FROM
    (
    SELECT 
            CONCAT('FY', DD.ACCOUNTING_YEAR,'P',LPAD(DD.ACCOUNTING_MONTH_NUMBER,2,'0'),'W',DD.FINANCIAL_WEEK_IN_PERIOD) AS FY_PW,
            DENSE_RANK() OVER(ORDER BY FY_PW DESC) AS FYP_RANK,
            LOC.STATE_NAME AS STATE,
            LOC.STORE_LOCATED_STATE_CODE AS STATE_ABV,
            COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate) AS FK_CREATED_DATE,
            pos.p_storenumber AS store_no,
    		ofctype.code AS ofc_type,
            dm.p_code as delivery_mode,
    	    CASE WHEN rejectReason.code = 'CANCELLED_BY_CUSTOMER' THEN 0
    		WHEN ofctype.code =  'INSTORE_PICKUP' AND 
    			 constatus.code <> 'CANCELLED' THEN COUNT(DISTINCT(con.p_code))
    		WHEN ofctype.code <> 'INSTORE_PICKUP' AND 
    		     constatus.code NOT IN ('CANCELLED', 'PICKED') THEN COUNT(DISTINCT(con.p_code))
    		     ELSE 0 END AS con_count,		 
        	CASE WHEN rejectReason.code = 'CANCELLED_BY_CUSTOMER' THEN 0
    		WHEN ofctype.code = 'INSTORE_PICKUP' THEN SUM(coen.p_quantity)
    		WHEN ofctype.code <> 'INSTORE_PICKUP' AND 
    		     constatus.code NOT IN ('PICKED') THEN SUM(coen.p_quantity) 
                 ELSE 0 END  AS wk_p_qty,		 
    	    CASE WHEN rejectReason.code = 'CANCELLED_BY_CUSTOMER' THEN 0
    		WHEN ofctype.code = 'INSTORE_PICKUP' THEN SUM(coen.p_shippedQuantity)
    		WHEN ofctype.code <> 'INSTORE_PICKUP' AND 
    		 	constatus.code NOT IN ('CANCELLED', 'PICKED') THEN SUM(coen.p_shippedQuantity) 
                ELSE 0 END AS wk_s_qty,
    	    SUM((NVL(coen.p_shippedquantity, 0) * (tmp0.adj_item_base_price - tmp0.discount_per_item)) -
    	    (NVL(coen.p_shippedquantity, 0) * (tmp0.adj_item_base_price - tmp0.discount_per_item) / 11)) AS wk_net_sales --         Net sales = base price - tax - discount.
    
    FROM KSFPA.ONLINE_UGAM_PVT.ldb_order_line_results_01 tmp0 
    	LEFT JOIN TSFPA.HYBRIS.VH_CONSIGNMENTS con ON con.p_order = tmp0.ord_pk
    	LEFT JOIN  TSFPA.HYBRIS.VH_CONSIGNMENTENTRIES coen ON (coen.p_orderentry = tmp0.oren_pk AND coen.p_consignment = con.pk)
    	LEFT JOIN TSFPA.HYBRIS.VH_ORDERS_ALL AS ord ON con.p_order = ord.PK
    	LEFT JOIN TSFPA.HYBRIS.VC_WAREHOUSES AS wh on wh.pk = con.p_warehouse
    	LEFT JOIN TSFPA.HYBRIS.VC_POS2WAREHOUSEREL AS pwr ON  pwr.TargetPK = wh.PK
    	LEFT JOIN TSFPA.HYBRIS.VC_POINTOFSERVICE AS pos  ON pos.pk = pwr.SourcePK
    	LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES conStatus ON con.p_status = conStatus.PK
    	LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES AS ofcType ON con.p_ofcorderdeliverytype = ofcType.PK
    	LEFT JOIN TSFPA.HYBRIS.VH_ENUMERATIONVALUES AS rejectReason ON con.p_rejectreason = rejectReason.PK
        LEFT JOIN TSFPA.HYBRIS.DELIVERYMODES dm ON dm.PK = ord.p_deliverymode
        LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE AS DD 
        ON DATE(con.p_packeddate) = DD.DATE 
        LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION AS LOC
        ON pos.p_storenumber = LOC.LOCATION_CODE
    				
    	WHERE COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate)::DATE >= '2023-01-01'
        --AND COALESCE(con.p_canceldate, con.p_packeddate, con.p_pickconfirmdate)::DATE <= '2024-04-28'
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
        
    	GROUP BY  FY_PW, FK_CREATED_DATE, STATE, STATE_ABV, pos.p_storenumber, ofctype.code, delivery_mode, constatus.code, rejectreason.code
    
    )
    group by 1,2,3,4,5,6,7;