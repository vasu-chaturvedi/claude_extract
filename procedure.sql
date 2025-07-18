CREATE OR REPLACE TYPE t_leg_row AS OBJECT (
    sol_id      VARCHAR2(20),
    txn_id      VARCHAR2(20),
    amount      NUMBER(14,2),
    dr_cr       CHAR(1)
);
/

CREATE OR REPLACE TYPE t_leg_tab AS TABLE OF t_leg_row;
/

CREATE OR REPLACE PROCEDURE MOCKSTG.extract_debit_chunk_with_credits (
    p_sol_id      IN  VARCHAR2,
    p_chunk_num   IN  NUMBER,
    p_chunk_size  IN  NUMBER,
    p_out_cursor  OUT SYS_REFCURSOR
) AS
	
	TYPE placeholder_sum IS TABLE OF NUMBER INDEX BY VARCHAR2(100);
    sums placeholder_sum;

    v_credit_rec  t_leg_row;
    v_results     t_leg_tab := t_leg_tab();
    v_debit_count NUMBER := 0;
    v_credit_count NUMBER := 0;
    v_start_row   NUMBER := ((p_chunk_num-1) * p_chunk_size) + 1;
    v_end_row     NUMBER := p_chunk_num * p_chunk_size;
    
    KEY varchar2(100);
    CURSOR c_debits IS
        SELECT * FROM 
		(
			SELECT 
				MPI_APP_REF_NO
				,SOL_ID
				,round((MPF_TOT_DISB_AMT - (MPF_PRIN_PREPAID + MPF_INST_PRIN_PAID)),2) TranAmt
				,D.FIN_PLACE_HOLDER
				,ROW_NUMBER() OVER (ORDER BY MPI_APP_REF_NO) rn
			FROM VALID_RA A,LMS.M_PROJ_FINBAL B,LAA_PROD_MAP C,GL_MAPPING D
			WHERE A.LMS_CUST_ID = B.MPF_CUST_ID
			AND A.LMS_SCHM_CD = c.OLD_SCHM_CODE
			AND A.MPI_CON_ID = B.MPF_CON_ID
			AND A.MPI_PROJ_ID = B.MPF_PROJ_ID
			AND D.PRODUCT_GLACC_CODE = C.PRODUCT_GLACC_CODE
			--AND A.MPI_APP_REF_NO IN ('1234120000006750','1234120000006744','1234120000006802','1234120000006770')
			AND A.SOL_ID = p_sol_id
			AND (MPF_TOT_DISB_AMT - (MPF_PRIN_PREPAID + MPF_INST_PRIN_PAID)) != 0
		)
        WHERE rn BETWEEN v_start_row AND v_end_row;
    CURSOR c_credits IS
    SELECT SOL_ID,FIN_PLACE_HOLDER,SUM(TRANAMT) TranAmt FROM (SELECT * FROM 
		(
			SELECT 
				MPI_APP_REF_NO
				,SOL_ID
				,round((MPF_TOT_DISB_AMT - (MPF_PRIN_PREPAID + MPF_INST_PRIN_PAID)),2) TranAmt
				,D.FIN_PLACE_HOLDER
				,ROW_NUMBER() OVER (ORDER BY MPI_APP_REF_NO) rn
			FROM VALID_RA A,LMS.M_PROJ_FINBAL B,LAA_PROD_MAP C,GL_MAPPING D
			WHERE A.LMS_CUST_ID = B.MPF_CUST_ID
			AND A.LMS_SCHM_CD = c.OLD_SCHM_CODE
			AND A.MPI_CON_ID = B.MPF_CON_ID
			AND A.MPI_PROJ_ID = B.MPF_PROJ_ID
			AND D.PRODUCT_GLACC_CODE = C.PRODUCT_GLACC_CODE
			--AND A.MPI_APP_REF_NO IN ('1234120000006750','1234120000006744','1234120000006802','1234120000006770')
			AND A.SOL_ID = p_sol_id
			AND (MPF_TOT_DISB_AMT - (MPF_PRIN_PREPAID + MPF_INST_PRIN_PAID)) != 0
		)
        WHERE rn BETWEEN 0 AND 2000)
		GROUP BY SOL_ID,FIN_PLACE_HOLDER;
BEGIN
    -- Loop over debits in this chunk
    FOR debit_rec IN c_debits LOOP
        -- Add DEBIT leg to output
        v_results.EXTEND;
        v_results(v_results.COUNT) := t_leg_row(
            debit_rec.sol_id,
            debit_rec.MPI_APP_REF_NO,
            debit_rec.TranAmt,
            'D'
        );
    END LOOP;
		
	FOR credit_rec IN c_credits LOOP
        -- Add DEBIT leg to output
        v_results.EXTEND;
        v_results(v_results.COUNT) := t_leg_row(
            credit_rec.sol_id,
            credit_rec.FIN_PLACE_HOLDER,
            credit_rec.TranAmt,
            'C'
        );
    END LOOP;
		
	

    -- Output the result as a cursor
    OPEN p_out_cursor FOR
        SELECT *
        FROM TABLE(v_results);
END;
