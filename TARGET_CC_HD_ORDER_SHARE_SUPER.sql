create or replace view KSFPA.ONLINE_UGAM_PVT.TARGET_CC_HD_ORDER_SHARE_SUPER(
	FY_PW,
	COUNT_ORD_NO,
	COUNT_CON_NO,
	DELIVERY_MODE
) as 

SELECT 

    CONCAT('FY',        
    cal.ACCOUNTING_YEAR,'P',LPAD(cal.ACCOUNTING_MONTH_NUMBER,2,'0'),'W',cal.FINANCIAL_WEEK_IN_PERIOD) AS FY_PW,
    COUNT(DISTINCT(ord.p_code)) as count_ord_no,
    COUNT(DISTINCT(con.p_code)) as count_con_no,
    dm.p_code as delivery_mode

FROM 
    
    TSFPA.HYBRIS.VH_ORDERS_ALL ord
    LEFT JOIN TSFPA.HYBRIS.VH_CONSIGNMENTS con ON con.p_order = ord.pk
    LEFT JOIN TSFPA.HYBRIS.DELIVERYMODES dm ON dm.PK = ord.p_deliverymode
    LEFT JOIN TSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE cal 
    ON cal.DATE = COALESCE(con.p_packeddate,con.p_pickconfirmdate)::DATE

WHERE 
    COALESCE(con.p_packeddate, con.p_pickconfirmdate) >= '2023-01-01 00:00:00'
    AND ord.p_originalversion IS NULL
    AND ord.p_fraudulent <> 1
    AND con.p_canceldate IS null
    AND ORD.CIR_ISCURRENT=TRUE AND ORD.CIR_ISDELETED=FALSE
    AND CON.CIR_ISCURRENT=TRUE AND CON.CIR_ISDELETED=FALSE

GROUP BY 
    FY_PW, dm.p_code;