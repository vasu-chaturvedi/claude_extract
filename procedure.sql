CREATE OR REPLACE TYPE t_leg_row AS OBJECT (
    sol_id      VARCHAR2(20),
    txn_id      VARCHAR2(20),
    amount      NUMBER,
    dr_cr       CHAR(1),
    scheme_code VARCHAR2(20),
    gl_code     VARCHAR2(20)
    -- add more fields as per your template
);
/

CREATE OR REPLACE TYPE t_leg_tab AS TABLE OF t_leg_row;
/

CREATE OR REPLACE PROCEDURE extract_debit_chunk_with_credits (
    p_sol_id      IN  VARCHAR2,
    p_chunk_num   IN  NUMBER,
    p_chunk_size  IN  NUMBER,
    p_out_cursor  OUT SYS_REFCURSOR
) AS
    v_debit_rec   debit_table%ROWTYPE;
    v_credit_rec  t_leg_row;
    v_results     t_leg_tab := t_leg_tab();
    v_debit_count NUMBER := 0;
    v_credit_count NUMBER := 0;
    v_start_row   NUMBER := ((p_chunk_num-1) * p_chunk_size) + 1;
    v_end_row     NUMBER := p_chunk_num * p_chunk_size;
    CURSOR c_debits IS
        SELECT *
        FROM (
            SELECT d.*, ROW_NUMBER() OVER (ORDER BY d.debit_id) rn
            FROM debit_table d
            WHERE d.sol_id = p_sol_id
        )
        WHERE rn BETWEEN v_start_row AND v_end_row;
BEGIN
    -- Loop over debits in this chunk
    FOR debit_rec IN c_debits LOOP
        -- Add DEBIT leg to output
        v_results.EXTEND;
        v_results(v_results.COUNT) := t_leg_row(
            debit_rec.sol_id,
            debit_rec.debit_id,
            debit_rec.amount,
            'D',
            debit_rec.scheme_code,
            debit_rec.gl_code
            -- add more fields as needed
        );
        v_debit_count := v_debit_count + 1;

        -- Now, for each debit, generate credits per mapping (can be multiple)
        FOR credit_map_rec IN (
            SELECT *
            FROM scheme_map sm
            JOIN gl_map gm ON sm.gl_code = gm.gl_code
            WHERE sm.scheme_code = debit_rec.scheme_code
              AND gm.gl_code = debit_rec.gl_code
              -- add any further matching rules needed
        ) LOOP
            -- For each mapping, generate one credit (expand as per your rules)
            v_results.EXTEND;
            v_results(v_results.COUNT) := t_leg_row(
                debit_rec.sol_id,
                debit_rec.debit_id,
                -- Example: simple amount, replace as needed
                debit_rec.amount, 
                'C',
                credit_map_rec.scheme_code,
                credit_map_rec.gl_code
                -- add more fields as needed
            );
            v_credit_count := v_credit_count + 1;
        END LOOP;
    END LOOP;

    -- Optionally: Validate balancing
    IF v_debit_count != v_credit_count THEN
        -- Or check sum(amount) if required
        RAISE_APPLICATION_ERROR(-20001, 'Debit and Credit count mismatch in this chunk!');
    END IF;

    -- Output the result as a cursor
    OPEN p_out_cursor FOR
        SELECT *
        FROM TABLE(v_results);
END;
/
