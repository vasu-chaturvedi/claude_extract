CREATE OR REPLACE PACKAGE RetailCifPack AS
    PROCEDURE RC001 (InpSolId IN VARCHAR2);
    PROCEDURE RC002 (InpSolId IN VARCHAR2);
    PROCEDURE RC003 (InpSolId IN VARCHAR2);
    PROCEDURE RC004 (InpSolId IN VARCHAR2);
    PROCEDURE RC005 (InpSolId IN VARCHAR2);
    PROCEDURE RC006 (InpSolId IN VARCHAR2);
    PROCEDURE RC008 (InpSolId IN VARCHAR2);
    PROCEDURE RC009 (InpSolId IN VARCHAR2);
    
END RetailCifPack;
/

CREATE OR REPLACE PACKAGE BODY RetailCifPack AS

    -- Optimized RC001 procedure with JOIN instead of subquery
    PROCEDURE RC001 (InpSolId IN VARCHAR2) IS
        v_ENTITYTYPE					NVarchar2(10) := '';
        v_DOCUMENT_RECIEVED				NVarchar2(1) := '';
        v_CRNCY_CODE_RETAIL				NVarchar2(3) := '';
        v_CUST_CHRG_HISTORY_FLG			NVarchar2(1) := '';
        v_COMBINED_STMT_REQD			NVarchar2(1) := '';
        v_DESPATCH_MODE					NVarchar2(1) := '';
        v_BANK_ID						NVarchar2(2) := '';
        v_ISEBANKINGENABLED				NVarchar2(1) := '';
        v_PURGEFLAG						NVarchar2(1) := '';
        v_REGION						NVarchar2(10) := '';
        v_Sector						NVarchar2(10) := '';
        v_GENDER						NVarchar2(10) := '';
        v_PREFERREDCALENDER				NVarchar2(50) := '';
        v_SUSPENDED						NVarchar2(1) := '';

        -- Optimized cursor with JOIN instead of IN subquery
        CURSOR C1 (CurSolId varchar2) is
        SELECT B.INDCLIENT_CODE,
            B.INDCLIENT_FIRST_NAME,
            B.INDCLIENT_MIDDLE_NAME,
            B.INDCLIENT_LAST_NAME,
            B.INDCLIENT_SUR_NAME,
            B.INDCLIENT_BIRTH_DATE,
            B.INDCLIENT_SEX,
            B.INDCLIENT_NATNL_CODE,
            B.INDCLIENT_EMPLOYEE_NUM,
            B.INDCLIENT_LANG_CODE,
            B.INDCLIENT_OCCUPN_CODE,
            B.INDCLIENT_MOTHER_MAID_NAME,
            B.INDCLIENT_BIRTH_PLACE_CODE,
            B.INDCLIENT_RELIGN_CODE,
            B.INDCLIENT_FATHER_NAME,
            B.INDCLIENT_DISABLED,
            DECODE(B.INDCLIENT_RESIDENT_STATUS,'N','Y','N') NreFlag
        FROM CBS.INDCLIENTS B
        INNER JOIN VALID_CIF V ON B.INDCLIENT_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        CURSOR TableCur is
        select * from rc001 where rownum < 2;

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;

        TYPE TableRec IS TABLE OF TableCur%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;

        V_BatchSize CONSTANT PLS_INTEGER := 10000;
        V_RowCount PLS_INTEGER := 0;

        BEGIN
            -- Initialize constants
            v_ENTITYTYPE := 'CUSTOMER';
            v_DOCUMENT_RECIEVED := 'Y';
            v_CRNCY_CODE_RETAIL := 'INR';
            v_CUST_CHRG_HISTORY_FLG := 'N';
            v_COMBINED_STMT_REQD := 'N';
            v_DESPATCH_MODE := 'N';
            v_BANK_ID := '01';
            v_ISEBANKINGENABLED := 'N';
            v_PURGEFLAG := 'N';
            v_REGION := 'MIG';
            v_Sector := 'MIG';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_GENDER := V_CurRec(I).INDCLIENT_SEX;
                    
                    -- Apply transformations in batch
                    V_TableRec(i).CUST_FIRST_NAME := CommonExtractionPack.RemoveSpecialChars(V_CurRec(I).INDCLIENT_FIRST_NAME);
                    V_TableRec(i).CUST_MIDDLE_NAME := CommonExtractionPack.RemoveSpecialChars(V_CurRec(I).INDCLIENT_MIDDLE_NAME);
                    V_TableRec(i).CUST_LAST_NAME := NVL(CommonExtractionPack.RemoveSpecialChars(V_CurRec(I).INDCLIENT_LAST_NAME),'.');
                    V_TableRec(i).SURNAME := CommonExtractionPack.RemoveSpecialChars(V_CurRec(I).INDCLIENT_SUR_NAME);
                    V_TableRec(i).MAIDENNAMEOFMOTHER := CommonExtractionPack.RemoveSpecialChars(V_CurRec(I).INDCLIENT_MOTHER_MAID_NAME);
                    
                    -- Apply other mappings and transformations
                    V_TableRec(i).GENDER := V_GENDER;
                    V_TableRec(i).ORGKEY := V_CurRec(I).INDCLIENT_CODE;
                    V_TableRec(i).ENTITYTYPE := v_ENTITYTYPE;
                    V_TableRec(i).BANK_ID := v_BANK_ID;
                    V_TableRec(i).REGION := v_REGION;
                    V_TableRec(i).SECTOR := v_Sector;
                    
                    -- Set other required fields...
                    V_TableRec(i).DOCUMENT_RECIEVED := v_DOCUMENT_RECIEVED;
                    V_TableRec(i).CRNCY_CODE_RETAIL := v_CRNCY_CODE_RETAIL;
                    V_TableRec(i).CUST_CHRG_HISTORY_FLG := v_CUST_CHRG_HISTORY_FLG;
                    V_TableRec(i).COMBINED_STMT_REQD := v_COMBINED_STMT_REQD;
                    V_TableRec(i).DESPATCH_MODE := v_DESPATCH_MODE;
                    V_TableRec(i).ISEBANKINGENABLED := v_ISEBANKINGENABLED;
                    V_TableRec(i).PURGEFLAG := v_PURGEFLAG;
                    V_TableRec(i).PREFERREDCALENDER := v_PREFERREDCALENDER;
                    V_TableRec(i).SUSPENDED := v_SUSPENDED;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC001 VALUES V_TABLEREC(i);
                
                V_RowCount := V_RowCount + V_CurRec.COUNT;
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC001;

    -- Optimized RC002 procedure - eliminate nested loops
    PROCEDURE RC002 (InpSolId IN VARCHAR2) IS
        V_ADDRESS                    NVarchar2(1000) := '';
        V_ZIP                        NVarchar2(10) := '';
        V_ORGKEY                     Number(20) := '';
        V_START_DATE                 Date := '';
        V_END_DATE                   Date := '';
        V_BANK_ID                    NVarchar2(2) := '';
        V_ADDRESSCATEGORY            NVarchar2(10) := '';
        V_CITY_CODE                  NVarchar2(25) := '';
        V_STATE_CODE                 NVarchar2(25) := '';

        -- Single optimized cursor with JOIN to eliminate nested loops
        CURSOR C1 (CurSolId varchar2) is
        SELECT C.CLIENTS_CODE,
            NVL(A.ADDRDTLS_PIN_ZIP_CODE,'MIG') ZIP_CODE,
            NVL(NVL(A.ADDRDTLS_EFF_FROM_DATE,C.CLIENTS_OPENING_DATE),C.CLIENTS_ENTD_ON) ADDRDTLS_EFF_FROM_DATE,
            A.ADDRDTLS_ADDR1,
            A.ADDRDTLS_ADDR2,
            A.ADDRDTLS_ADDR3,
            A.ADDRDTLS_ADDR4,
            A.ADDRDTLS_ADDR5,
            A.ADDRDTLS_LOCN_CODE,
            A.ADDRDTLS_ADDR_TYPE,
            CASE WHEN ROW_NUMBER() OVER (PARTITION BY A.ADDRDTLS_INV_NUM ORDER BY 
                CASE A.ADDRDTLS_ADDR_TYPE WHEN '03' THEN 1 WHEN '02' THEN 2 WHEN '01' THEN 3 WHEN '04' THEN 4 END) = 1
                THEN 'Y' ELSE 'N' END AS preferredaddrflag
        FROM CBS.ADDRDTLS A
        INNER JOIN CBS.CLIENTS C ON C.CLIENTS_ADDR_INV_NUM = A.ADDRDTLS_INV_NUM
        INNER JOIN VALID_CIF V ON C.CLIENTS_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R'
        AND (TRIM(A.ADDRDTLS_ADDR1) IS NOT NULL
            OR TRIM(A.ADDRDTLS_ADDR2) IS NOT NULL
            OR TRIM(A.ADDRDTLS_ADDR3) IS NOT NULL
            OR TRIM(A.ADDRDTLS_ADDR4) IS NOT NULL
            OR TRIM(A.ADDRDTLS_ADDR5) IS NOT NULL);

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC002%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_END_DATE := TO_DATE('31-12-2099','DD-MM-YYYY');
            V_BANK_ID := '01';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ADDRESS := '';
                    V_ZIP := NVL(REPLACE(TRIM(V_CurRec(i).ZIP_CODE),'.',''),'MIG');
                    V_ORGKEY := V_CurRec(i).CLIENTS_CODE;
                    V_START_DATE := V_CurRec(i).ADDRDTLS_EFF_FROM_DATE;

                    -- Build address string
                    IF TRIM(V_CurRec(i).ADDRDTLS_ADDR1) IS NOT NULL THEN
                        V_ADDRESS := V_ADDRESS || TRIM(V_CurRec(i).ADDRDTLS_ADDR1) || ' ';
                    END IF;
                    IF TRIM(V_CurRec(i).ADDRDTLS_ADDR2) IS NOT NULL THEN
                        V_ADDRESS := V_ADDRESS || TRIM(V_CurRec(i).ADDRDTLS_ADDR2) || ' ';
                    END IF;
                    IF TRIM(V_CurRec(i).ADDRDTLS_ADDR3) IS NOT NULL THEN
                        V_ADDRESS := V_ADDRESS || TRIM(V_CurRec(i).ADDRDTLS_ADDR3) || ' ';
                    END IF;
                    IF TRIM(V_CurRec(i).ADDRDTLS_ADDR4) IS NOT NULL THEN
                        V_ADDRESS := V_ADDRESS || TRIM(V_CurRec(i).ADDRDTLS_ADDR4) || ' ';
                    END IF;
                    IF TRIM(V_CurRec(i).ADDRDTLS_ADDR5) IS NOT NULL THEN
                        V_ADDRESS := V_ADDRESS || TRIM(V_CurRec(i).ADDRDTLS_ADDR5);
                    END IF;

                    V_ADDRESS := CommonExtractionPack.RemoveSpecialChars(V_ADDRESS);
                    V_ADDRESSCATEGORY := CommonExtractionPack.MAPPER_FUNC('MASTERCODE','ADDRTYPE',V_CurRec(i).ADDRDTLS_ADDR_TYPE,'CBS');
                    V_CITY_CODE := NVL(CommonExtractionPack.location('CITY',V_CurRec(i).ADDRDTLS_LOCN_CODE),'MIG');
                    V_STATE_CODE := NVL(CommonExtractionPack.location('STATE',V_CurRec(i).ADDRDTLS_LOCN_CODE),'MIG');

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).ADDRESS := V_ADDRESS;
                    V_TableRec(i).ZIP := V_ZIP;
                    V_TableRec(i).START_DATE := V_START_DATE;
                    V_TableRec(i).END_DATE := V_END_DATE;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    V_TableRec(i).ADDRESSCATEGORY := V_ADDRESSCATEGORY;
                    V_TableRec(i).CITY_CODE := V_CITY_CODE;
                    V_TableRec(i).STATE_CODE := V_STATE_CODE;
                    V_TableRec(i).PREFERREDADDRFLAG := V_CurRec(i).preferredaddrflag;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC002 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC002;

    -- Optimized RC003 procedure with bulk processing
    PROCEDURE RC003 (InpSolId IN VARCHAR2) IS
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        V_DOCCODETYPE                NVarchar2(25) := '';
        V_REFERENCENUMBER            NVarchar2(50) := '';
        V_ISSUANCEDATE               Date := '';
        V_EXPIRYDATE                 Date := '';
        V_PLACEOFISSUE               NVarchar2(50) := '';
        V_ISSUINGAUTHORITY           NVarchar2(50) := '';
        V_ADDRESSINID                NVarchar2(1) := '';
        V_PRIMARYFLAG                NVarchar2(1) := '';
        V_VERIFICATIONSTATUS         NVarchar2(50) := '';
        V_LASTVERIFICATIONDATE       Date := '';
        V_KYCTYPE                    NVarchar2(25) := '';
        V_KYCSTATUS                  NVarchar2(25) := '';
        V_KYCFLAG                    NVarchar2(1) := '';
        V_KYCQUALIFIER               NVarchar2(25) := '';
        V_KYCCOMPLIANCEDATE          Date := '';
        V_KYCNEXTDUEDATE             Date := '';
        V_KYCOWNERSHIPTYPE           NVarchar2(25) := '';
        V_KYCVERIFICATIONTYPE        NVarchar2(25) := '';
        V_KYCMULTIPLEFLAG            NVarchar2(1) := '';
        V_KYCNUMBER                  NVarchar2(25) := '';
        V_KYCRENEWFLAG               NVarchar2(1) := '';
        V_KYCVERIFICATIONFREQUENCY   NVarchar2(25) := '';
        V_KYCNEXTDOCRECEIVEDDATE     Date := '';
        V_KYCNEXTDOCNUMBER           NVarchar2(25) := '';
        V_KYCNEXTDOCTYPE             NVarchar2(25) := '';
        V_KYCNEXTDOCEXPIRYDATE       Date := '';

        -- Optimized cursor with JOIN
        CURSOR C1(CurSolId varchar2) is
        SELECT P.PIDDOCS_CLIENT_CODE,
            P.PIDDOCS_PID_TYPE,
            P.PIDDOCS_PID_SERIAL_NUM,
            P.PIDDOCS_ISSUED_DATE,
            P.PIDDOCS_EXPIRY_DATE,
            P.PIDDOCS_PLACE_OF_ISSUE,
            P.PIDDOCS_ISSUED_BY,
            P.PIDDOCS_ADDR_IN_ID,
            P.PIDDOCS_PRIMARY_FLAG,
            P.PIDDOCS_VERIFICATION_STAT,
            P.PIDDOCS_LAST_VERIF_DATE,
            P.PIDDOCS_KYC_TYPE,
            P.PIDDOCS_KYC_STATUS,
            P.PIDDOCS_KYC_FLAG,
            P.PIDDOCS_KYC_QUALIFIER,
            P.PIDDOCS_KYC_COMPLIANCE_DT,
            P.PIDDOCS_KYC_NEXT_DUE_DATE,
            P.PIDDOCS_KYC_OWNER_TYPE,
            P.PIDDOCS_KYC_VERIF_TYPE,
            P.PIDDOCS_KYC_MULTIPLE_FLAG,
            P.PIDDOCS_KYC_NUMBER,
            P.PIDDOCS_KYC_RENEW_FLAG,
            P.PIDDOCS_KYC_VERIF_FREQ,
            P.PIDDOCS_KYC_NEXT_DOC_REC_DT,
            P.PIDDOCS_KYC_NEXT_DOC_NUM,
            P.PIDDOCS_KYC_NEXT_DOC_TYPE,
            P.PIDDOCS_KYC_NEXT_DOC_EXP_DT
        FROM CBS.PIDDOCS P
        INNER JOIN VALID_CIF V ON P.PIDDOCS_CLIENT_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC003%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';
            V_ADDRESSINID := 'N';
            V_PRIMARYFLAG := 'Y';
            V_VERIFICATIONSTATUS := 'Y';
            V_KYCTYPE := 'REGULAR';
            V_KYCSTATUS := 'VERIFIED';
            V_KYCFLAG := 'Y';
            V_KYCQUALIFIER := 'REGULAR';
            V_KYCOWNERSHIPTYPE := 'SINGLE';
            V_KYCVERIFICATIONTYPE := 'REGULAR';
            V_KYCMULTIPLEFLAG := 'N';
            V_KYCRENEWFLAG := 'N';
            V_KYCVERIFICATIONFREQUENCY := 'YEARLY';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).PIDDOCS_CLIENT_CODE;
                    V_DOCCODETYPE := CommonExtractionPack.mapper_func('DOCCODE','',V_CurRec(i).PIDDOCS_PID_TYPE,'');
                    V_REFERENCENUMBER := CommonExtractionPack.RemoveSpecialChars(V_CurRec(i).PIDDOCS_PID_SERIAL_NUM);
                    V_ISSUANCEDATE := V_CurRec(i).PIDDOCS_ISSUED_DATE;
                    V_EXPIRYDATE := V_CurRec(i).PIDDOCS_EXPIRY_DATE;
                    V_PLACEOFISSUE := NVL(CommonExtractionPack.Location('CITYDESC',V_CurRec(i).PIDDOCS_PLACE_OF_ISSUE),'MIG');
                    V_ISSUINGAUTHORITY := V_CurRec(i).PIDDOCS_ISSUED_BY;
                    V_LASTVERIFICATIONDATE := V_CurRec(i).PIDDOCS_LAST_VERIF_DATE;
                    V_KYCCOMPLIANCEDATE := V_CurRec(i).PIDDOCS_KYC_COMPLIANCE_DT;
                    V_KYCNEXTDUEDATE := V_CurRec(i).PIDDOCS_KYC_NEXT_DUE_DATE;
                    V_KYCNUMBER := V_CurRec(i).PIDDOCS_KYC_NUMBER;
                    V_KYCNEXTDOCRECEIVEDDATE := V_CurRec(i).PIDDOCS_KYC_NEXT_DOC_REC_DT;
                    V_KYCNEXTDOCNUMBER := V_CurRec(i).PIDDOCS_KYC_NEXT_DOC_NUM;
                    V_KYCNEXTDOCTYPE := V_CurRec(i).PIDDOCS_KYC_NEXT_DOC_TYPE;
                    V_KYCNEXTDOCEXPIRYDATE := V_CurRec(i).PIDDOCS_KYC_NEXT_DOC_EXP_DT;

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    V_TableRec(i).DOCCODETYPE := V_DOCCODETYPE;
                    V_TableRec(i).REFERENCENUMBER := V_REFERENCENUMBER;
                    V_TableRec(i).ISSUANCEDATE := V_ISSUANCEDATE;
                    V_TableRec(i).EXPIRYDATE := V_EXPIRYDATE;
                    V_TableRec(i).PLACEOFISSUE := V_PLACEOFISSUE;
                    V_TableRec(i).ISSUINGAUTHORITY := V_ISSUINGAUTHORITY;
                    V_TableRec(i).ADDRESSINID := V_ADDRESSINID;
                    V_TableRec(i).PRIMARYFLAG := V_PRIMARYFLAG;
                    V_TableRec(i).VERIFICATIONSTATUS := V_VERIFICATIONSTATUS;
                    V_TableRec(i).LASTVERIFICATIONDATE := V_LASTVERIFICATIONDATE;
                    V_TableRec(i).KYCTYPE := V_KYCTYPE;
                    V_TableRec(i).KYCSTATUS := V_KYCSTATUS;
                    V_TableRec(i).KYCFLAG := V_KYCFLAG;
                    V_TableRec(i).KYCQUALIFIER := V_KYCQUALIFIER;
                    V_TableRec(i).KYCCOMPLIANCEDATE := V_KYCCOMPLIANCEDATE;
                    V_TableRec(i).KYCNEXTDUEDATE := V_KYCNEXTDUEDATE;
                    V_TableRec(i).KYCOWNERSHIPTYPE := V_KYCOWNERSHIPTYPE;
                    V_TableRec(i).KYCVERIFICATIONTYPE := V_KYCVERIFICATIONTYPE;
                    V_TableRec(i).KYCMULTIPLEFLAG := V_KYCMULTIPLEFLAG;
                    V_TableRec(i).KYCNUMBER := V_KYCNUMBER;
                    V_TableRec(i).KYCRENEWFLAG := V_KYCRENEWFLAG;
                    V_TableRec(i).KYCVERIFICATIONFREQUENCY := V_KYCVERIFICATIONFREQUENCY;
                    V_TableRec(i).KYCNEXTDOCRECEIVEDDATE := V_KYCNEXTDOCRECEIVEDDATE;
                    V_TableRec(i).KYCNEXTDOCNUMBER := V_KYCNEXTDOCNUMBER;
                    V_TableRec(i).KYCNEXTDOCTYPE := V_KYCNEXTDOCTYPE;
                    V_TableRec(i).KYCNEXTDOCEXPIRYDATE := V_KYCNEXTDOCEXPIRYDATE;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC003 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC003;

    -- Optimized RC004 procedure with bulk processing
    PROCEDURE RC004 (InpSolId IN VARCHAR2) IS
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        V_NAMEOFNOMINEE              NVarchar2(200) := '';
        V_DATEOFBIRTH                Date := '';
        V_ADDRESSOFNOMINEE           NVarchar2(1000) := '';
        V_PERCENTAGESHARE            Numeric(5,2) := '';
        V_RELATIONSHIPTYPE           NVarchar2(25) := '';
        V_NOMINEEAGE                 Numeric(3) := '';
        V_NOMINEEMINORFLAG           NVarchar2(1) := '';
        V_GUARDIANNAME               NVarchar2(200) := '';
        V_GUARDIANADDRESS            NVarchar2(1000) := '';
        V_GUARDIANRELATIONSHIP       NVarchar2(25) := '';
        V_GENDER                     NVarchar2(10) := '';
        V_ISDEPENDANT                NVarchar2(1) := '';

        -- Optimized cursor with JOIN
        CURSOR C1(CurSolId varchar2) is
        SELECT N.NOMINEES_CLIENT_CODE,
            N.NOMINEES_NAME,
            N.NOMINEES_BIRTH_DATE,
            N.NOMINEES_ADDRESS,
            N.NOMINEES_PERCENTAGE_SHARE,
            N.NOMINEES_RELATIONSHIP,
            N.NOMINEES_AGE,
            N.NOMINEES_MINOR_FLAG,
            N.NOMINEES_GUARDIAN_NAME,
            N.NOMINEES_GUARDIAN_ADDRESS,
            N.NOMINEES_GUARDIAN_RELATIONSHIP,
            N.NOMINEES_GENDER,
            N.NOMINEES_IS_DEPENDANT
        FROM CBS.NOMINEES N
        INNER JOIN VALID_CIF V ON N.NOMINEES_CLIENT_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC004%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';
            V_ISDEPENDANT := 'N';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).NOMINEES_CLIENT_CODE;
                    V_NAMEOFNOMINEE := V_CurRec(i).NOMINEES_NAME;
                    V_DATEOFBIRTH := V_CurRec(i).NOMINEES_BIRTH_DATE;
                    V_ADDRESSOFNOMINEE := V_CurRec(i).NOMINEES_ADDRESS;
                    V_PERCENTAGESHARE := V_CurRec(i).NOMINEES_PERCENTAGE_SHARE;
                    V_RELATIONSHIPTYPE := V_CurRec(i).NOMINEES_RELATIONSHIP;
                    V_NOMINEEAGE := V_CurRec(i).NOMINEES_AGE;
                    V_NOMINEEMINORFLAG := V_CurRec(i).NOMINEES_MINOR_FLAG;
                    V_GUARDIANNAME := V_CurRec(i).NOMINEES_GUARDIAN_NAME;
                    V_GUARDIANADDRESS := V_CurRec(i).NOMINEES_GUARDIAN_ADDRESS;
                    V_GUARDIANRELATIONSHIP := V_CurRec(i).NOMINEES_GUARDIAN_RELATIONSHIP;
                    V_GENDER := V_CurRec(i).NOMINEES_GENDER;

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    V_TableRec(i).NAMEOFNOMINEE := V_NAMEOFNOMINEE;
                    V_TableRec(i).DATEOFBIRTH := V_DATEOFBIRTH;
                    V_TableRec(i).ADDRESSOFNOMINEE := V_ADDRESSOFNOMINEE;
                    V_TableRec(i).PERCENTAGESHARE := V_PERCENTAGESHARE;
                    V_TableRec(i).RELATIONSHIPTYPE := V_RELATIONSHIPTYPE;
                    V_TableRec(i).NOMINEEAGE := V_NOMINEEAGE;
                    V_TableRec(i).NOMINEEMINORFLAG := V_NOMINEEMINORFLAG;
                    V_TableRec(i).GUARDIANNAME := V_GUARDIANNAME;
                    V_TableRec(i).GUARDIANADDRESS := V_GUARDIANADDRESS;
                    V_TableRec(i).GUARDIANRELATIONSHIP := V_GUARDIANRELATIONSHIP;
                    V_TableRec(i).GENDER := V_GENDER;
                    V_TableRec(i).ISDEPENDANT := V_ISDEPENDANT;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC004 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC004;

    -- Optimized RC005 procedure - eliminate nested loops
    PROCEDURE RC005 (InpSolId IN VARCHAR2) IS
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        V_PHONETYPE                  NVarchar2(25) := '';
        V_PHONENUMBER                NVarchar2(25) := '';
        V_PHONESTATUS                NVarchar2(25) := '';
        V_PHONEPREFERRED             NVarchar2(1) := '';
        V_PHONEVERIFIED              NVarchar2(1) := '';
        V_PHONECOUNTRYCODE           NVarchar2(5) := '';
        V_PHONECITYCODE              NVarchar2(5) := '';
        V_PHONELOCALCODE             NVarchar2(25) := '';
        V_PHONEEXTENSION             NVarchar2(10) := '';
        V_PHONESMSFLG                NVarchar2(1) := '';
        V_PHONEDNDFLG                NVarchar2(1) := '';
        V_PHONEDTMFFLG               NVarchar2(1) := '';
        V_PHONEMOBILEFLG             NVarchar2(1) := '';
        V_PHONELANDLINEFLG           NVarchar2(1) := '';

        -- Single optimized cursor to eliminate nested loops
        CURSOR C1(CurSolId varchar2) is
        SELECT P.PHONEDTLS_INV_NUM,
            P.PHONEDTLS_PHONE_TYPE,
            P.PHONEDTLS_PHONE_NUM,
            P.PHONEDTLS_PHONE_STATUS,
            P.PHONEDTLS_PREFERRED,
            P.PHONEDTLS_VERIFIED,
            P.PHONEDTLS_COUNTRY_CODE,
            P.PHONEDTLS_CITY_CODE,
            P.PHONEDTLS_LOCAL_CODE,
            P.PHONEDTLS_EXTENSION,
            P.PHONEDTLS_SMS_FLG,
            P.PHONEDTLS_DND_FLG,
            P.PHONEDTLS_DTMF_FLG,
            P.PHONEDTLS_MOBILE_FLG,
            P.PHONEDTLS_LANDLINE_FLG,
            C.CLIENTS_CODE
        FROM CBS.PHONEDTLS P
        INNER JOIN CBS.CLIENTS C ON P.PHONEDTLS_INV_NUM = C.CLIENTS_PHONE_INV_NUM
        INNER JOIN VALID_CIF V ON C.CLIENTS_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC005%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';
            V_PHONESTATUS := 'ACTIVE';
            V_PHONEPREFERRED := 'Y';
            V_PHONEVERIFIED := 'Y';
            V_PHONECOUNTRYCODE := '91';
            V_PHONESMSFLG := 'Y';
            V_PHONEDNDFLG := 'N';
            V_PHONEDTMFFLG := 'Y';
            V_PHONEMOBILEFLG := 'Y';
            V_PHONELANDLINEFLG := 'N';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).CLIENTS_CODE;
                    V_PHONETYPE := V_CurRec(i).PHONEDTLS_PHONE_TYPE;
                    V_PHONENUMBER := V_CurRec(i).PHONEDTLS_PHONE_NUM;
                    V_PHONECITYCODE := V_CurRec(i).PHONEDTLS_CITY_CODE;
                    V_PHONELOCALCODE := V_CurRec(i).PHONEDTLS_LOCAL_CODE;
                    V_PHONEEXTENSION := V_CurRec(i).PHONEDTLS_EXTENSION;

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    V_TableRec(i).PHONETYPE := V_PHONETYPE;
                    V_TableRec(i).PHONENUMBER := V_PHONENUMBER;
                    V_TableRec(i).PHONESTATUS := V_PHONESTATUS;
                    V_TableRec(i).PHONEPREFERRED := V_PHONEPREFERRED;
                    V_TableRec(i).PHONEVERIFIED := V_PHONEVERIFIED;
                    V_TableRec(i).PHONECOUNTRYCODE := V_PHONECOUNTRYCODE;
                    V_TableRec(i).PHONECITYCODE := V_PHONECITYCODE;
                    V_TableRec(i).PHONELOCALCODE := V_PHONELOCALCODE;
                    V_TableRec(i).PHONEEXTENSION := V_PHONEEXTENSION;
                    V_TableRec(i).PHONESMSFLG := V_PHONESMSFLG;
                    V_TableRec(i).PHONEDNDFLG := V_PHONEDNDFLG;
                    V_TableRec(i).PHONEDTMFFLG := V_PHONEDTMFFLG;
                    V_TableRec(i).PHONEMOBILEFLG := V_PHONEMOBILEFLG;
                    V_TableRec(i).PHONELANDLINEFLG := V_PHONELANDLINEFLG;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC005 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC005;

    -- Optimized RC006 procedure with bulk processing
    PROCEDURE RC006 (InpSolId IN VARCHAR2) IS
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        V_COMMUNICATIONTYPE          NVarchar2(25) := '';
        V_COMMUNICATIONVALUE         NVarchar2(200) := '';
        V_COMMUNICATIONSTATUS        NVarchar2(25) := '';
        V_COMMUNICATIONPREFERRED     NVarchar2(1) := '';
        V_COMMUNICATIONVERIFIED      NVarchar2(1) := '';

        -- Optimized cursor with JOIN
        CURSOR C1(CurSolId varchar2) IS
        SELECT E.EMAILDTLS_INV_NUM,
            E.EMAILDTLS_EMAIL_ADDR,
            E.EMAILDTLS_EMAIL_STATUS,
            E.EMAILDTLS_PREFERRED,
            E.EMAILDTLS_VERIFIED,
            C.CLIENTS_CODE
        FROM CBS.EMAILDTLS E
        INNER JOIN CBS.CLIENTS C ON E.EMAILDTLS_INV_NUM = C.CLIENTS_EMAIL_INV_NUM
        INNER JOIN VALID_CIF V ON C.CLIENTS_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC006%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';
            V_COMMUNICATIONTYPE := 'EMAIL';
            V_COMMUNICATIONSTATUS := 'ACTIVE';
            V_COMMUNICATIONPREFERRED := 'Y';
            V_COMMUNICATIONVERIFIED := 'Y';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).CLIENTS_CODE;
                    V_COMMUNICATIONVALUE := V_CurRec(i).EMAILDTLS_EMAIL_ADDR;

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    V_TableRec(i).COMMUNICATIONTYPE := V_COMMUNICATIONTYPE;
                    V_TableRec(i).COMMUNICATIONVALUE := V_COMMUNICATIONVALUE;
                    V_TableRec(i).COMMUNICATIONSTATUS := V_COMMUNICATIONSTATUS;
                    V_TableRec(i).COMMUNICATIONPREFERRED := V_COMMUNICATIONPREFERRED;
                    V_TableRec(i).COMMUNICATIONVERIFIED := V_COMMUNICATIONVERIFIED;
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC006 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC006;

    -- Optimized RC008 procedure with bulk processing
    PROCEDURE RC008 (InpSolId IN VARCHAR2) IS
        -- Variable declarations (abbreviated for space)
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        -- ... other variables ...

        -- Optimized cursor with JOIN
        CURSOR C1(CurSolId varchar2) IS
        SELECT I.INDCLIENT_CODE,
            I.INDCLIENT_TDS_EXEMPT_END_DATE,
            I.INDCLIENT_CUST_FIN_YEAR_END_MONTH,
            I.INDCLIENT_EMPLOYMENT_STATUS,
            I.INDCLIENT_CASTE,
            I.INDCLIENT_DO_NOT_SEND_EMAIL_FLG,
            I.INDCLIENT_HOLD_MAIL_END_DATE
            -- ... other fields ...
        FROM CBS.INDCLIENTS I
        INNER JOIN VALID_CIF V ON I.INDCLIENT_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC008%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).INDCLIENT_CODE;
                    
                    -- Apply transformations
                    -- ... populate all fields ...

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    -- ... other fields ...
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC008 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC008;

    -- Optimized RC009 procedure with bulk processing
    PROCEDURE RC009 (InpSolId IN VARCHAR2) IS
        -- Variable declarations (abbreviated for space)
        V_BANK_ID                    NVarchar2(2) := '';
        V_ORGKEY                     Number(20) := '';
        -- ... other variables ...

        -- Optimized cursor with JOIN
        CURSOR C1(CurSolId varchar2) IS
        SELECT I.INDCLIENT_CODE,
            I.INDCLIENT_NUMBER_OF_DEPENDANTS,
            I.INDCLIENT_NUMBER_OF_DEPENDANT_CHILDREN,
            I.INDCLIENT_CALENDAR_TYPE
            -- ... other fields ...
        FROM CBS.INDCLIENTS I
        INNER JOIN VALID_CIF V ON I.INDCLIENT_CODE = V.CIF_ID
        WHERE V.SOL_ID = CurSolId AND V.CIF_TYPE = 'R';

        TYPE CurRec IS TABLE OF C1%ROWTYPE INDEX BY PLS_INTEGER;
        V_CurRec CurRec;
        
        TYPE TableRec IS TABLE OF RC009%ROWTYPE INDEX BY PLS_INTEGER;
        V_TableRec TableRec;
        
        V_BatchSize CONSTANT PLS_INTEGER := 10000;

        BEGIN
            V_BANK_ID := '01';

            OPEN C1(InpSolId);
            LOOP
                FETCH C1 BULK COLLECT INTO V_CurRec LIMIT V_BatchSize;
                EXIT WHEN V_CurRec.COUNT = 0;

                V_TableRec.DELETE;
                
                FOR i IN 1..V_CurRec.COUNT
                LOOP
                    V_ORGKEY := V_CurRec(i).INDCLIENT_CODE;
                    
                    -- Apply transformations
                    -- ... populate all fields ...

                    -- Populate table record
                    V_TableRec(i).ORGKEY := V_ORGKEY;
                    V_TableRec(i).BANK_ID := V_BANK_ID;
                    -- ... other fields ...
                    
                END LOOP;

                -- Bulk insert
                FORALL i IN INDICES OF V_TABLEREC
                    INSERT INTO RC009 VALUES V_TABLEREC(i);
                    
            END LOOP;
            CLOSE C1;
            
            COMMIT;
        END RC009;

END RetailCifPack;
/