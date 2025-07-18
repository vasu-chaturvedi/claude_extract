-- 1. Define types matching your output template

CREATE OR REPLACE TYPE t_leg_row AS OBJECT (
    sol_id      VARCHAR2(20),
    txn_id      VARCHAR2(20),
    amount      NUMBER,
    dr_cr       CHAR(1),       -- 'D' or 'C'
    scheme_code VARCHAR2(20),
    gl_code     VARCHAR2(20)
    -- Add more fields as per your template
);
/

CREATE OR REPLACE TYPE t_leg_tab AS TABLE OF t_leg_row;
/

-- 2. The chunked, balanced procedure

CREATE OR REPLACE PROCEDURE extract_debit_chunk_with_credits (
    p_sol_id      IN  VARCHAR2,
    p_chunk_num   IN  NUMBER,
    p_chunk_size  IN  NUMBER,
    p_out_cursor  OUT SYS_REFCURSOR
) AS
BEGIN
    OPEN p_out_cursor FOR
    WITH debits_chunk AS (
        SELECT *
        FROM (
            SELECT d.*, ROW_NUMBER() OVER (ORDER BY d.debit_id) rn
            FROM debit_table d
            WHERE d.sol_id = p_sol_id
        )
        WHERE rn BETWEEN ((p_chunk_num - 1) * p_chunk_size + 1)
                     AND (p_chunk_num * p_chunk_size)
    ),
    credits_chunk AS (
        -- One or more credits per debit, as per mapping
        SELECT
            d.sol_id,
            d.debit_id        AS txn_id,
            -- Example: full amount as credit, flag as credit
            d.amount          AS amount,
            'C'               AS dr_cr,
            sm.scheme_code    AS scheme_code,
            gm.gl_code        AS gl_code
            -- Add/transform more fields as needed
        FROM debits_chunk d
        JOIN scheme_map sm ON d.scheme_code = sm.scheme_code
        JOIN gl_map gm     ON d.gl_code = gm.gl_code
        -- Add any additional mapping logic or joins here
    )
    SELECT
        d.sol_id,
        d.debit_id        AS txn_id,
        d.amount,
        'D'               AS dr_cr,
        d.scheme_code,
        d.gl_code
        -- More fields as per template
    FROM debits_chunk d

    UNION ALL

    SELECT
        c.sol_id,
        c.txn_id,
        c.amount,
        c.dr_cr,
        c.scheme_code,
        c.gl_code
        -- More fields as per template
    FROM credits_chunk c;
END;
/
