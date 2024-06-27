create or replace view KSFPA.ONLINE_UGAM_PVT.OP_CNC_TARGET_SUPER(
	FY_PW,
	STORE_NO,
	STATE,
	STATE_ABV,
	C_ONEPASS,
	CNC_SLA_ACHEIVED_COUNT,
	COUNT_CON_NO,
	OFC_TYPE,
	NET_CON_COUNT,
	SUM_UNIT_P_QTY,
	SUM_UNIT_S_QTY,
	NET_SALES
) as
                
 SELECT
    FY_PW,
    STORE_NO,
    STATE,
    STATE_ABV,
 	c_onepass,
 	SUM(cnc_sla_achieved_count) AS cnc_sla_acheived_count,
 	COUNT(DISTINCT(con_no)) AS count_con_no,
 	ofc_type,
 	SUM(con_count) AS net_con_count,
 	SUM(day_p_qty) AS sum_unit_p_qty,
 	SUM(day_s_qty) AS sum_unit_s_qty,
 	SUM(day_net_sales) AS net_sales
 	
 FROM KSFPA.ONLINE_UGAM_PVT.ldb_10_day_picking_sla
 GROUP BY  FY_PW, STORE_NO,STATE, STATE_ABV, c_onepass, ofc_type;