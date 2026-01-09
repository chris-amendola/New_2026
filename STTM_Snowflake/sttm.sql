-- =====================================================================
-- SEMANTIC TABLE AND COLUMN MAPPING SOLUTION FOR SNOWFLAKE
-- =====================================================================
-- This solution provides AI-powered and traditional semantic matching
-- between source and target tables/columns with configurable parameters
-- =====================================================================

-- Step 1: Create schema for mapping objects
CREATE SCHEMA IF NOT EXISTS SEMANTIC_MAPPING;
USE SCHEMA SEMANTIC_MAPPING;

-- =====================================================================
-- CONFIGURATION TABLES
-- =====================================================================

-- Configuration table for matching parameters
CREATE OR REPLACE TABLE CONFIG_MATCHING_PARAMETERS (
    PARAMETER_NAME VARCHAR(100) PRIMARY KEY,
    PARAMETER_VALUE VARCHAR(500),
    DESCRIPTION VARCHAR(1000),
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert default configuration
INSERT INTO CONFIG_MATCHING_PARAMETERS VALUES
    ('MATCHING_METHOD', 'TRADITIONAL', 'TRADITIONAL or CORTEX_LLM', CURRENT_TIMESTAMP()),
    ('CONFIDENCE_THRESHOLD', '0.60', 'Minimum confidence score (0-1)', CURRENT_TIMESTAMP()),
    ('TOP_N_MATCHES', '5', 'Number of candidate matches to return', CURRENT_TIMESTAMP()),
    ('WEIGHT_EXACT_MATCH', '1.0', 'Weight for exact name matches', CURRENT_TIMESTAMP()),
    ('WEIGHT_FUZZY_MATCH', '0.7', 'Weight for fuzzy string similarity', CURRENT_TIMESTAMP()),
    ('WEIGHT_TOKEN_MATCH', '0.8', 'Weight for token-based matching', CURRENT_TIMESTAMP()),
    ('WEIGHT_PHONETIC_MATCH', '0.5', 'Weight for phonetic similarity', CURRENT_TIMESTAMP()),
    ('WEIGHT_SYNONYM_MATCH', '0.9', 'Weight for synonym/abbreviation matches', CURRENT_TIMESTAMP()),
    ('WEIGHT_DATATYPE_MATCH', '0.3', 'Weight for compatible data types', CURRENT_TIMESTAMP());

-- Abbreviation and synonym dictionary
CREATE OR REPLACE TABLE DICT_ABBREVIATIONS (
    ABBREVIATION VARCHAR(100),
    FULL_TERM VARCHAR(100),
    CATEGORY VARCHAR(50)
);

INSERT INTO DICT_ABBREVIATIONS VALUES
    -- Common business abbreviations
    ('CUST', 'CUSTOMER', 'BUSINESS'),
    ('ACCT', 'ACCOUNT', 'BUSINESS'),
    ('ADDR', 'ADDRESS', 'BUSINESS'),
    ('NUM', 'NUMBER', 'GENERAL'),
    ('NO', 'NUMBER', 'GENERAL'),
    ('NBR', 'NUMBER', 'GENERAL'),
    ('ID', 'IDENTIFIER', 'GENERAL'),
    ('DESC', 'DESCRIPTION', 'GENERAL'),
    ('QTY', 'QUANTITY', 'BUSINESS'),
    ('AMT', 'AMOUNT', 'BUSINESS'),
    ('DT', 'DATE', 'TEMPORAL'),
    ('TM', 'TIME', 'TEMPORAL'),
    ('TS', 'TIMESTAMP', 'TEMPORAL'),
    ('YR', 'YEAR', 'TEMPORAL'),
    ('MO', 'MONTH', 'TEMPORAL'),
    ('DAY', 'DAY', 'TEMPORAL'),
    ('STR', 'STREET', 'ADDRESS'),
    ('ST', 'STREET', 'ADDRESS'),
    ('AVE', 'AVENUE', 'ADDRESS'),
    ('BLVD', 'BOULEVARD', 'ADDRESS'),
    ('CTY', 'CITY', 'ADDRESS'),
    ('ZIP', 'ZIPCODE', 'ADDRESS'),
    ('POSTAL', 'ZIPCODE', 'ADDRESS'),
    ('PROV', 'PROVINCE', 'ADDRESS'),
    ('CTRY', 'COUNTRY', 'ADDRESS'),
    ('PHN', 'PHONE', 'CONTACT'),
    ('PH', 'PHONE', 'CONTACT'),
    ('TEL', 'TELEPHONE', 'CONTACT'),
    ('FAX', 'FACSIMILE', 'CONTACT'),
    ('EML', 'EMAIL', 'CONTACT'),
    ('PROD', 'PRODUCT', 'BUSINESS'),
    ('CAT', 'CATEGORY', 'BUSINESS'),
    ('ORD', 'ORDER', 'BUSINESS'),
    ('INV', 'INVOICE', 'BUSINESS'),
    ('PYMT', 'PAYMENT', 'BUSINESS'),
    ('TAX', 'TAXATION', 'BUSINESS'),
    ('DISC', 'DISCOUNT', 'BUSINESS'),
    ('PCT', 'PERCENT', 'GENERAL'),
    ('SRC', 'SOURCE', 'TECHNICAL'),
    ('TGT', 'TARGET', 'TECHNICAL'),
    ('SEQ', 'SEQUENCE', 'TECHNICAL'),
    ('IND', 'INDICATOR', 'TECHNICAL'),
    ('FLG', 'FLAG', 'TECHNICAL'),
    ('CD', 'CODE', 'TECHNICAL'),
    ('TYP', 'TYPE', 'TECHNICAL'),
    ('STAT', 'STATUS', 'TECHNICAL'),
    ('PROC', 'PROCESS', 'TECHNICAL'),
    ('TRANS', 'TRANSACTION', 'BUSINESS'),
    ('REF', 'REFERENCE', 'GENERAL'),
    ('CURR', 'CURRENT', 'TEMPORAL'),
    ('PREV', 'PREVIOUS', 'TEMPORAL'),
    ('ORIG', 'ORIGINAL', 'GENERAL'),
    ('TEMP', 'TEMPORARY', 'TECHNICAL'),
    ('PERM', 'PERMANENT', 'TECHNICAL');

-- Schema and table exclusion list
CREATE OR REPLACE TABLE CONFIG_EXCLUSIONS (
    EXCLUSION_TYPE VARCHAR(20), -- 'SCHEMA' or 'TABLE'
    EXCLUSION_PATTERN VARCHAR(500),
    DESCRIPTION VARCHAR(1000)
);

-- =====================================================================
-- OUTPUT TABLES
-- =====================================================================

-- Main mapping results table
CREATE OR REPLACE TABLE MAPPING_RESULTS (
    MAPPING_ID NUMBER AUTOINCREMENT,
    SOURCE_DATABASE VARCHAR(256),
    SOURCE_SCHEMA VARCHAR(256),
    SOURCE_TABLE VARCHAR(256),
    SOURCE_COLUMN VARCHAR(256),
    SOURCE_DATA_TYPE VARCHAR(256),
    TARGET_DATABASE VARCHAR(256),
    TARGET_SCHEMA VARCHAR(256),
    TARGET_TABLE VARCHAR(256),
    TARGET_COLUMN VARCHAR(256),
    TARGET_DATA_TYPE VARCHAR(256),
    CONFIDENCE_SCORE FLOAT,
    MATCH_RANK NUMBER,
    TRANSFORMATION_SQL VARCHAR(4000),
    MATCHING_METHOD VARCHAR(50),
    MATCH_DETAILS VARIANT,
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (MAPPING_ID)
);

-- Unmapped columns tracking
CREATE OR REPLACE TABLE UNMAPPED_COLUMNS (
    UNMAPPED_ID NUMBER AUTOINCREMENT,
    SIDE VARCHAR(10), -- 'SOURCE' or 'TARGET'
    DATABASE_NAME VARCHAR(256),
    SCHEMA_NAME VARCHAR(256),
    TABLE_NAME VARCHAR(256),
    COLUMN_NAME VARCHAR(256),
    DATA_TYPE VARCHAR(256),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (UNMAPPED_ID)
);

-- =====================================================================
-- HELPER FUNCTIONS
-- =====================================================================

-- Function to calculate Jaro-Winkler similarity
CREATE OR REPLACE FUNCTION JARO_WINKLER_SIMILARITY(str1 VARCHAR, str2 VARCHAR)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    if (!STR1 || !STR2) return 0;
    
    var s1 = STR1.toUpperCase();
    var s2 = STR2.toUpperCase();
    
    if (s1 === s2) return 1.0;
    
    var len1 = s1.length;
    var len2 = s2.length;
    
    var maxDist = Math.floor(Math.max(len1, len2) / 2) - 1;
    var matches = 0;
    var transpositions = 0;
    var s1Matches = new Array(len1).fill(false);
    var s2Matches = new Array(len2).fill(false);
    
    // Find matches
    for (var i = 0; i < len1; i++) {
        var start = Math.max(0, i - maxDist);
        var end = Math.min(i + maxDist + 1, len2);
        
        for (var j = start; j < end; j++) {
            if (s2Matches[j] || s1[i] !== s2[j]) continue;
            s1Matches[i] = true;
            s2Matches[j] = true;
            matches++;
            break;
        }
    }
    
    if (matches === 0) return 0;
    
    // Find transpositions
    var k = 0;
    for (var i = 0; i < len1; i++) {
        if (!s1Matches[i]) continue;
        while (!s2Matches[k]) k++;
        if (s1[i] !== s2[k]) transpositions++;
        k++;
    }
    
    var jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3;
    
    // Winkler modification
    var prefix = 0;
    for (var i = 0; i < Math.min(len1, len2, 4); i++) {
        if (s1[i] === s2[i]) prefix++;
        else break;
    }
    
    return jaro + prefix * 0.1 * (1 - jaro);
$$;

-- Function to calculate Levenshtein distance normalized
CREATE OR REPLACE FUNCTION LEVENSHTEIN_SIMILARITY(str1 VARCHAR, str2 VARCHAR)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    if (!STR1 || !STR2) return 0;
    
    var s1 = STR1.toUpperCase();
    var s2 = STR2.toUpperCase();
    
    if (s1 === s2) return 1.0;
    
    var len1 = s1.length;
    var len2 = s2.length;
    
    var matrix = [];
    
    for (var i = 0; i <= len1; i++) {
        matrix[i] = [i];
    }
    
    for (var j = 0; j <= len2; j++) {
        matrix[0][j] = j;
    }
    
    for (var i = 1; i <= len1; i++) {
        for (var j = 1; j <= len2; j++) {
            var cost = s1[i - 1] === s2[j - 1] ? 0 : 1;
            matrix[i][j] = Math.min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost
            );
        }
    }
    
    var distance = matrix[len1][len2];
    var maxLen = Math.max(len1, len2);
    
    return 1 - (distance / maxLen);
$$;

-- Function to calculate phonetic similarity using Soundex
CREATE OR REPLACE FUNCTION SOUNDEX_CODE(str VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    if (!STR || STR.length === 0) return '';
    
    var s = STR.toUpperCase().replace(/[^A-Z]/g, '');
    if (s.length === 0) return '';
    
    var soundex = s[0];
    var codes = {
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6'
    };
    
    var prev = codes[s[0]] || '0';
    
    for (var i = 1; i < s.length && soundex.length < 4; i++) {
        var code = codes[s[i]] || '0';
        if (code !== '0' && code !== prev) {
            soundex += code;
        }
        prev = code;
    }
    
    while (soundex.length < 4) soundex += '0';
    
    return soundex.substring(0, 4);
$$;

-- Tokenization function
CREATE OR REPLACE FUNCTION TOKENIZE_COLUMN_NAME(col_name VARCHAR)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS
$$
    if (!COL_NAME) return [];
    
    // Split on underscores, spaces, and camelCase
    var tokens = COL_NAME
        .replace(/([a-z])([A-Z])/g, '$1_$2')
        .split(/[_\s\-\.]+/)
        .filter(function(t) { return t.length > 0; })
        .map(function(t) { return t.toUpperCase(); });
    
    return tokens;
$$;

-- =====================================================================
-- TRADITIONAL MATCHING PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE CALCULATE_TRADITIONAL_MATCH(
    source_col VARCHAR,
    target_col VARCHAR,
    source_type VARCHAR,
    target_type VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    // Get configuration weights
    var weights = {};
    var stmt = snowflake.createStatement({
        sqlText: "SELECT PARAMETER_NAME, PARAMETER_VALUE FROM SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS WHERE PARAMETER_NAME LIKE 'WEIGHT_%'"
    });
    var rs = stmt.execute();
    while (rs.next()) {
        weights[rs.getColumnValue(1)] = parseFloat(rs.getColumnValue(2));
    }
    
    var scores = {
        exact: 0,
        fuzzy: 0,
        token: 0,
        phonetic: 0,
        synonym: 0,
        datatype: 0
    };
    
    // Exact match
    if (SOURCE_COL.toUpperCase() === TARGET_COL.toUpperCase()) {
        scores.exact = 1.0;
    }
    
    // Fuzzy string similarity (Jaro-Winkler)
    var fuzzyStmt = snowflake.createStatement({
        sqlText: "SELECT SEMANTIC_MAPPING.JARO_WINKLER_SIMILARITY(?, ?)",
        binds: [SOURCE_COL, TARGET_COL]
    });
    var fuzzyRs = fuzzyStmt.execute();
    if (fuzzyRs.next()) {
        scores.fuzzy = fuzzyRs.getColumnValue(1);
    }
    
    // Token-based matching
    var tokenStmt = snowflake.createStatement({
        sqlText: `
            WITH src AS (SELECT VALUE AS token FROM TABLE(FLATTEN(SEMANTIC_MAPPING.TOKENIZE_COLUMN_NAME(?)))),
                 tgt AS (SELECT VALUE AS token FROM TABLE(FLATTEN(SEMANTIC_MAPPING.TOKENIZE_COLUMN_NAME(?))))
            SELECT 
                COUNT(DISTINCT src.token) AS src_count,
                COUNT(DISTINCT tgt.token) AS tgt_count,
                COUNT(DISTINCT CASE WHEN tgt.token IS NOT NULL THEN src.token END) AS match_count
            FROM src
            LEFT JOIN tgt ON src.token = tgt.token
        `,
        binds: [SOURCE_COL, TARGET_COL]
    });
    var tokenRs = tokenStmt.execute();
    if (tokenRs.next()) {
        var srcCnt = tokenRs.getColumnValue(1);
        var tgtCnt = tokenRs.getColumnValue(2);
        var matchCnt = tokenRs.getColumnValue(3);
        if (srcCnt > 0 && tgtCnt > 0) {
            scores.token = (2 * matchCnt) / (srcCnt + tgtCnt);
        }
    }
    
    // Phonetic similarity
    var phoneticStmt = snowflake.createStatement({
        sqlText: "SELECT SEMANTIC_MAPPING.SOUNDEX_CODE(?) = SEMANTIC_MAPPING.SOUNDEX_CODE(?)",
        binds: [SOURCE_COL, TARGET_COL]
    });
    var phoneticRs = phoneticStmt.execute();
    if (phoneticRs.next()) {
        scores.phonetic = phoneticRs.getColumnValue(1) ? 1.0 : 0.0;
    }
    
    // Synonym/abbreviation matching
    var synonymStmt = snowflake.createStatement({
        sqlText: `
            WITH src_tokens AS (SELECT VALUE AS token FROM TABLE(FLATTEN(SEMANTIC_MAPPING.TOKENIZE_COLUMN_NAME(?)))),
                 tgt_tokens AS (SELECT VALUE AS token FROM TABLE(FLATTEN(SEMANTIC_MAPPING.TOKENIZE_COLUMN_NAME(?)))),
                 expanded_src AS (
                     SELECT COALESCE(d.FULL_TERM, s.token) AS term
                     FROM src_tokens s
                     LEFT JOIN SEMANTIC_MAPPING.DICT_ABBREVIATIONS d ON s.token = d.ABBREVIATION
                 ),
                 expanded_tgt AS (
                     SELECT COALESCE(d.FULL_TERM, t.token) AS term
                     FROM tgt_tokens t
                     LEFT JOIN SEMANTIC_MAPPING.DICT_ABBREVIATIONS d ON t.token = d.ABBREVIATION
                 )
            SELECT 
                COUNT(DISTINCT es.term) AS src_count,
                COUNT(DISTINCT et.term) AS tgt_count,
                COUNT(DISTINCT CASE WHEN et.term IS NOT NULL THEN es.term END) AS match_count
            FROM expanded_src es
            LEFT JOIN expanded_tgt et ON es.term = et.term
        `,
        binds: [SOURCE_COL, TARGET_COL]
    });
    var synonymRs = synonymStmt.execute();
    if (synonymRs.next()) {
        var srcCnt = synonymRs.getColumnValue(1);
        var tgtCnt = synonymRs.getColumnValue(2);
        var matchCnt = synonymRs.getColumnValue(3);
        if (srcCnt > 0 && tgtCnt > 0) {
            scores.synonym = (2 * matchCnt) / (srcCnt + tgtCnt);
        }
    }
    
    // Data type compatibility
    var typeMap = {
        'NUMBER': ['NUMBER', 'NUMERIC', 'INTEGER', 'FLOAT', 'DECIMAL'],
        'VARCHAR': ['VARCHAR', 'STRING', 'TEXT', 'CHAR'],
        'DATE': ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ'],
        'BOOLEAN': ['BOOLEAN']
    };
    
    var srcBase = SOURCE_TYPE.split('(')[0].toUpperCase();
    var tgtBase = TARGET_TYPE.split('(')[0].toUpperCase();
    
    if (srcBase === tgtBase) {
        scores.datatype = 1.0;
    } else {
        for (var key in typeMap) {
            if (typeMap[key].indexOf(srcBase) >= 0 && typeMap[key].indexOf(tgtBase) >= 0) {
                scores.datatype = 0.8;
                break;
            }
        }
    }
    
    // Calculate weighted overall score
    var totalScore = 
        scores.exact * weights['WEIGHT_EXACT_MATCH'] +
        scores.fuzzy * weights['WEIGHT_FUZZY_MATCH'] +
        scores.token * weights['WEIGHT_TOKEN_MATCH'] +
        scores.phonetic * weights['WEIGHT_PHONETIC_MATCH'] +
        scores.synonym * weights['WEIGHT_SYNONYM_MATCH'] +
        scores.datatype * weights['WEIGHT_DATATYPE_MATCH'];
    
    var totalWeight = 
        weights['WEIGHT_EXACT_MATCH'] +
        weights['WEIGHT_FUZZY_MATCH'] +
        weights['WEIGHT_TOKEN_MATCH'] +
        weights['WEIGHT_PHONETIC_MATCH'] +
        weights['WEIGHT_SYNONYM_MATCH'] +
        weights['WEIGHT_DATATYPE_MATCH'];
    
    var finalScore = totalScore / totalWeight;
    
    return {
        confidence_score: finalScore,
        component_scores: scores,
        weights: weights
    };
$$;

-- =====================================================================
-- TRANSFORMATION SUGGESTION FUNCTION
-- =====================================================================

CREATE OR REPLACE FUNCTION SUGGEST_TRANSFORMATION(
    source_col VARCHAR,
    source_type VARCHAR,
    target_col VARCHAR,
    target_type VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var transformations = [];
    
    var srcType = SOURCE_TYPE.toUpperCase().split('(')[0];
    var tgtType = TARGET_TYPE.toUpperCase().split('(')[0];
    
    var srcCol = SOURCE_COL;
    var currentCol = srcCol;
    
    // Data type conversion
    if (srcType !== tgtType) {
        if (tgtType === 'NUMBER' || tgtType === 'NUMERIC' || tgtType === 'INTEGER') {
            currentCol = 'TRY_CAST(' + currentCol + ' AS NUMBER)';
            transformations.push('Type: VARCHAR->NUMBER');
        } else if (tgtType === 'DATE') {
            currentCol = 'TRY_TO_DATE(' + currentCol + ')';
            transformations.push('Type: ->DATE');
        } else if (tgtType.indexOf('TIMESTAMP') >= 0) {
            currentCol = 'TRY_TO_TIMESTAMP(' + currentCol + ')';
            transformations.push('Type: ->TIMESTAMP');
        } else if (tgtType === 'BOOLEAN') {
            currentCol = 'TRY_TO_BOOLEAN(' + currentCol + ')';
            transformations.push('Type: ->BOOLEAN');
        } else if (tgtType === 'VARCHAR' || tgtType === 'STRING') {
            currentCol = 'TO_VARCHAR(' + currentCol + ')';
            transformations.push('Type: ->VARCHAR');
        }
    }
    
    // Null handling
    currentCol = 'COALESCE(' + currentCol + ', NULL)';
    transformations.push('Null: COALESCE');
    
    // Case handling based on column name patterns
    var srcUpper = SOURCE_COL.toUpperCase();
    var tgtUpper = TARGET_COL.toUpperCase();
    
    if (srcUpper === srcUpper && tgtUpper !== tgtUpper) {
        // Source is uppercase, target is mixed case
        if (tgtType === 'VARCHAR' || tgtType === 'STRING') {
            currentCol = 'INITCAP(' + currentCol + ')';
            transformations.push('Case: INITCAP');
        }
    } else if (tgtUpper === tgtUpper && srcUpper !== srcUpper) {
        // Target is uppercase
        if (tgtType === 'VARCHAR' || tgtType === 'STRING') {
            currentCol = 'UPPER(' + currentCol + ')';
            transformations.push('Case: UPPER');
        }
    }
    
    // Trimming for string types
    if ((srcType === 'VARCHAR' || srcType === 'STRING') && 
        (tgtType === 'VARCHAR' || tgtType === 'STRING')) {
        currentCol = 'TRIM(' + currentCol + ')';
        transformations.push('Trim: TRIM');
    }
    
    return currentCol + ' AS ' + TARGET_COL + ' -- ' + transformations.join(', ');
$$;

-- =====================================================================
-- MAIN MAPPING PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE EXECUTE_SEMANTIC_MAPPING(
    source_database VARCHAR,
    source_schema VARCHAR,
    target_database VARCHAR,
    target_schema VARCHAR,
    output_schema VARCHAR DEFAULT 'SEMANTIC_MAPPING'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // Get configuration parameters
    var configStmt = snowflake.createStatement({
        sqlText: "SELECT PARAMETER_NAME, PARAMETER_VALUE FROM " + OUTPUT_SCHEMA + ".CONFIG_MATCHING_PARAMETERS"
    });
    var config = {};
    var configRs = configStmt.execute();
    while (configRs.next()) {
        config[configRs.getColumnValue(1)] = configRs.getColumnValue(2);
    }
    
    var matchingMethod = config['MATCHING_METHOD'];
    var confidenceThreshold = parseFloat(config['CONFIDENCE_THRESHOLD']);
    var topN = parseInt(config['TOP_N_MATCHES']);
    
    // Clear previous results
    snowflake.execute({sqlText: "TRUNCATE TABLE " + OUTPUT_SCHEMA + ".MAPPING_RESULTS"});
    snowflake.execute({sqlText: "TRUNCATE TABLE " + OUTPUT_SCHEMA + ".UNMAPPED_COLUMNS"});
    
    // Get exclusion patterns
    var exclusions = {schemas: [], tables: []};
    var exclStmt = snowflake.createStatement({
        sqlText: "SELECT EXCLUSION_TYPE, EXCLUSION_PATTERN FROM " + OUTPUT_SCHEMA + ".CONFIG_EXCLUSIONS"
    });
    var exclRs = exclStmt.execute();
    while (exclRs.next()) {
        var type = exclRs.getColumnValue(1);
        var pattern = exclRs.getColumnValue(2);
        if (type === 'SCHEMA') exclusions.schemas.push(pattern);
        if (type === 'TABLE') exclusions.tables.push(pattern);
    }
    
    if (matchingMethod === 'CORTEX_LLM') {
        // Cortex LLM-based matching
        var cortexSql = `
            INSERT INTO ${OUTPUT_SCHEMA}.MAPPING_RESULTS (
                SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, SOURCE_DATA_TYPE,
                TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TARGET_DATA_TYPE,
                CONFIDENCE_SCORE, MATCH_RANK, TRANSFORMATION_SQL, MATCHING_METHOD, MATCH_DETAILS
            )
            WITH source_cols AS (
                SELECT 
                    TABLE_CATALOG AS db,
                    TABLE_SCHEMA AS schema,
                    TABLE_NAME AS table,
                    COLUMN_NAME AS column,
                    DATA_TYPE AS data_type,
                    ORDINAL_POSITION
                FROM ${SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}'
            ),
            target_cols AS (
                SELECT 
                    TABLE_CATALOG AS db,
                    TABLE_SCHEMA AS schema,
                    TABLE_NAME AS table,
                    COLUMN_NAME AS column,
                    DATA_TYPE AS data_type,
                    ORDINAL_POSITION,
                    COMMENT
                FROM ${TARGET_DATABASE}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '${TARGET_SCHEMA}'
            ),
            embeddings AS (
                SELECT 
                    s.db AS src_db, s.schema AS src_schema, s.table AS src_table, 
                    s.column AS src_col, s.data_type AS src_type,
                    t.db AS tgt_db, t.schema AS tgt_schema, t.table AS tgt_table,
                    t.column AS tgt_col, t.data_type AS tgt_type, t.COMMENT AS tgt_comment,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', s.table || '.' || s.column) AS src_embedding,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
                        t.table || '.' || t.column || ' ' || COALESCE(t.COMMENT, '')) AS tgt_embedding
                FROM source_cols s
                CROSS JOIN target_cols t
            ),
            scored AS (
                SELECT 
                    src_db, src_schema, src_table, src_col, src_type,
                    tgt_db, tgt_schema, tgt_table, tgt_col, tgt_type,
                    VECTOR_COSINE_SIMILARITY(src_embedding, tgt_embedding) AS confidence_score,
                    ROW_NUMBER() OVER (PARTITION BY src_db, src_schema, src_table, src_col 
                                       ORDER BY VECTOR_COSINE_SIMILARITY(src_embedding, tgt_embedding) DESC) AS rn
                FROM embeddings
            )
            SELECT 
                src_db, src_schema, src_table, src_col, src_type,
                tgt_db, tgt_schema, tgt_table, tgt_col, tgt_type,
                confidence_score,
                rn,
                ${OUTPUT_SCHEMA}.SUGGEST_TRANSFORMATION(src_col, src_type, tgt_col, tgt_type),
                'CORTEX_LLM',
                OBJECT_CONSTRUCT('method', 'embedding_cosine_similarity')
            FROM scored
            WHERE confidence_score >= ${confidenceThreshold}
              AND rn <= ${topN}
        `;
        
        try {
            snowflake.execute({sqlText: cortexSql});
        } catch (err) {
            return "Error in Cortex LLM matching: " + err.message + ". Ensure Cortex functions are available in your account.";
        }
        
    } else {
        // Traditional matching
        var traditionalSql = `
            CREATE OR REPLACE TEMPORARY TABLE temp_source_cols AS
            SELECT 
                TABLE_CATALOG AS db,
                TABLE_SCHEMA AS schema,
                TABLE_NAME AS table,
                COLUMN_NAME AS column,
                DATA_TYPE AS data_type
            FROM ${SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}'
        `;
        snowflake.execute({sqlText: traditionalSql});
        
        var targetSql = `
            CREATE OR REPLACE TEMPORARY TABLE temp_target_cols AS
            SELECT 
                TABLE_CATALOG AS db,
                TABLE_SCHEMA AS schema,
                TABLE_NAME AS table,
                COLUMN_NAME AS column,
                DATA_TYPE AS data_type
            FROM ${TARGET_DATABASE}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${TARGET_SCHEMA}'
        `;
        snowflake.execute({sqlText: targetSql});
        
        // Process matches
        var processSql = `
            SELECT 
                s.db, s.schema, s.table, s.column, s.data_type,
                t.db, t.schema, t.table, t.column, t.data_type
            FROM temp_source_cols s
            CROSS JOIN temp_target_cols t
        `;
        
        var processStmt = snowflake.createStatement({sqlText: processSql});
        var processRs = processStmt.execute();
        
        var matches = {};
        
        while (processRs.next()) {
            var srcDb = processRs.getColumnValue(1);
            var srcSchema = processRs.getColumnValue(2);
            var srcTable = processRs.getColumnValue(3);
            var srcCol = processRs.getColumnValue(4);
            var srcType = processRs.getColumnValue(5);
            var tgtDb = processRs.getColumnValue(6);
            var tgtSchema = processRs.getColumnValue(7);
            var tgtTable = processRs.getColumnValue(8);
            var tgtCol = processRs.getColumnValue(9);
            var tgtType = processRs.getColumnValue(10);
            
            // Call matching function
            var matchStmt = snowflake.createStatement({
                sqlText: "CALL " + OUTPUT_SCHEMA + ".CALCULATE_TRADITIONAL_MATCH(?, ?, ?, ?)",
                binds: [srcCol, tgtCol, srcType, tgtType]
            });
            var matchRs = matchStmt.execute();
            matchRs.next();
            var result = JSON.parse(matchRs.getColumnValue(1));
            
            var score = result.confidence_score;
            
            if (score >= confidenceThreshold) {
                var key = srcDb + '|' + srcSchema + '|' + srcTable + '|' + srcCol;
                if (!matches[key]) matches[key] = [];
                
                matches[key].push({
                    srcDb: srcDb, srcSchema: srcSchema, srcTable: srcTable, srcCol: srcCol, srcType: srcType,
                    tgtDb: tgtDb, tgtSchema: tgtSchema, tgtTable: tgtTable, tgtCol: tgtCol, tgtType: tgtType,
                    score: score,
                    details: result
                });
            }
        }
        
        // Insert top N matches for each source column
        for (var key in matches) {
            var matchList = matches[key];
            matchList.sort(function(a, b) { return b.score - a.score; });
            
            for (var i = 0; i < Math.min(matchList.length, topN); i++) {
                var m = matchList[i];
                
                var transformStmt = snowflake.createStatement({
                    sqlText: "SELECT " + OUTPUT_SCHEMA + ".SUGGEST_TRANSFORMATION(?, ?, ?, ?)",
                    binds: [m.srcCol, m.srcType, m.tgtCol, m.tgtType]
                });
                var transformRs = transformStmt.execute();
                transformRs.next();
                var transformation = transformRs.getColumnValue(1);
                
                var insertStmt = snowflake.createStatement({
                    sqlText: `
                        INSERT INTO ${OUTPUT_SCHEMA}.MAPPING_RESULTS (
                            SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, SOURCE_DATA_TYPE,
                            TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TARGET_DATA_TYPE,
                            CONFIDENCE_SCORE, MATCH_RANK, TRANSFORMATION_SQL, MATCHING_METHOD, MATCH_DETAILS
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?))
                    `,
                    binds: [
                        m.srcDb, m.srcSchema, m.srcTable, m.srcCol, m.srcType,
                        m.tgtDb, m.tgtSchema, m.tgtTable, m.tgtCol, m.tgtType,
                        m.score, i + 1, transformation, 'TRADITIONAL',
                        JSON.stringify(m.details)
                    ]
                });
                insertStmt.execute();
            }
        }
    }
    
    // Find unmapped source columns
    var unmappedSourceSql = `
        INSERT INTO ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS (SIDE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE)
        SELECT 
            'SOURCE',
            s.TABLE_CATALOG,
            s.TABLE_SCHEMA,
            s.TABLE_NAME,
            s.COLUMN_NAME,
            s.DATA_TYPE
        FROM ${SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS s
        WHERE s.TABLE_SCHEMA = '${SOURCE_SCHEMA}'
          AND NOT EXISTS (
              SELECT 1 FROM ${OUTPUT_SCHEMA}.MAPPING_RESULTS m
              WHERE m.SOURCE_DATABASE = s.TABLE_CATALOG
                AND m.SOURCE_SCHEMA = s.TABLE_SCHEMA
                AND m.SOURCE_TABLE = s.TABLE_NAME
                AND m.SOURCE_COLUMN = s.COLUMN_NAME
                AND m.MATCH_RANK = 1
          )
    `;
    snowflake.execute({sqlText: unmappedSourceSql});
    
    // Find unmapped target columns
    var unmappedTargetSql = `
        INSERT INTO ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS (SIDE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE)
        SELECT 
            'TARGET',
            t.TABLE_CATALOG,
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.COLUMN_NAME,
            t.DATA_TYPE
        FROM ${TARGET_DATABASE}.INFORMATION_SCHEMA.COLUMNS t
        WHERE t.TABLE_SCHEMA = '${TARGET_SCHEMA}'
          AND NOT EXISTS (
              SELECT 1 FROM ${OUTPUT_SCHEMA}.MAPPING_RESULTS m
              WHERE m.TARGET_DATABASE = t.TABLE_CATALOG
                AND m.TARGET_SCHEMA = t.TABLE_SCHEMA
                AND m.TARGET_TABLE = t.TABLE_NAME
                AND m.TARGET_COLUMN = t.COLUMN_NAME
          )
    `;
    snowflake.execute({sqlText: unmappedTargetSql});
    
    // Get summary counts
    var summaryStmt = snowflake.createStatement({
        sqlText: `
            SELECT 
                COUNT(DISTINCT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE || '.' || SOURCE_COLUMN) AS mapped_source_cols,
                (SELECT COUNT(*) FROM ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS WHERE SIDE = 'SOURCE') AS unmapped_source_cols,
                (SELECT COUNT(*) FROM ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS WHERE SIDE = 'TARGET') AS unmapped_target_cols
            FROM ${OUTPUT_SCHEMA}.MAPPING_RESULTS
            WHERE MATCH_RANK = 1
        `
    });
    var summaryRs = summaryStmt.execute();
    summaryRs.next();
    
    return "Semantic mapping completed successfully. " +
           "Mapped: " + summaryRs.getColumnValue(1) + " source columns, " +
           "Unmapped: " + summaryRs.getColumnValue(2) + " source columns, " +
           summaryRs.getColumnValue(3) + " target columns. " +
           "Method: " + matchingMethod;
$;

-- =====================================================================
-- VIEWS FOR RESULTS
-- =====================================================================

-- View: Best matches only (rank 1)
CREATE OR REPLACE VIEW V_BEST_MATCHES AS
SELECT 
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    SOURCE_DATA_TYPE,
    TARGET_DATABASE,
    TARGET_SCHEMA,
    TARGET_TABLE,
    TARGET_COLUMN,
    TARGET_DATA_TYPE,
    CONFIDENCE_SCORE,
    TRANSFORMATION_SQL,
    MATCHING_METHOD,
    MATCH_DETAILS,
    CREATED_TIMESTAMP
FROM MAPPING_RESULTS
WHERE MATCH_RANK = 1
ORDER BY CONFIDENCE_SCORE DESC;

-- View: All candidate matches with rankings
CREATE OR REPLACE VIEW V_ALL_MATCH_CANDIDATES AS
SELECT 
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    SOURCE_DATA_TYPE,
    TARGET_DATABASE,
    TARGET_SCHEMA,
    TARGET_TABLE,
    TARGET_COLUMN,
    TARGET_DATA_TYPE,
    CONFIDENCE_SCORE,
    MATCH_RANK,
    TRANSFORMATION_SQL,
    MATCHING_METHOD,
    MATCH_DETAILS,
    CREATED_TIMESTAMP
FROM MAPPING_RESULTS
ORDER BY 
    SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, 
    MATCH_RANK;

-- View: Unmapped columns summary
CREATE OR REPLACE VIEW V_UNMAPPED_SUMMARY AS
SELECT 
    SIDE,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CREATED_TIMESTAMP
FROM UNMAPPED_COLUMNS
ORDER BY SIDE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME;

-- View: Mapping statistics
CREATE OR REPLACE VIEW V_MAPPING_STATISTICS AS
SELECT 
    MATCHING_METHOD,
    COUNT(DISTINCT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE) AS source_tables,
    COUNT(DISTINCT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE || '.' || SOURCE_COLUMN) AS source_columns_mapped,
    COUNT(DISTINCT TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE) AS target_tables,
    COUNT(DISTINCT TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE || '.' || TARGET_COLUMN) AS target_columns_matched,
    AVG(CONFIDENCE_SCORE) AS avg_confidence,
    MIN(CONFIDENCE_SCORE) AS min_confidence,
    MAX(CONFIDENCE_SCORE) AS max_confidence,
    COUNT(*) AS total_mappings
FROM MAPPING_RESULTS
WHERE MATCH_RANK = 1
GROUP BY MATCHING_METHOD;

-- =====================================================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- =====================================================================

/*
SETUP INSTRUCTIONS:
===================

1. Execute this entire script to create the schema, tables, functions, procedures, and views.

2. Configure matching parameters:
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = 'CORTEX_LLM'  -- or 'TRADITIONAL'
   WHERE PARAMETER_NAME = 'MATCHING_METHOD';
   
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = '0.70'
   WHERE PARAMETER_NAME = 'CONFIDENCE_THRESHOLD';
   
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = '3'
   WHERE PARAMETER_NAME = 'TOP_N_MATCHES';

3. (Optional) Add exclusions:
   INSERT INTO SEMANTIC_MAPPING.CONFIG_EXCLUSIONS VALUES
   ('SCHEMA', 'INFORMATION_SCHEMA', 'Exclude system schemas'),
   ('TABLE', '%_BACKUP', 'Exclude backup tables');

4. Execute the mapping:
   CALL SEMANTIC_MAPPING.EXECUTE_SEMANTIC_MAPPING(
       'SOURCE_DB',      -- Source database name
       'SOURCE_SCHEMA',  -- Source schema name
       'TARGET_DB',      -- Target database name
       'TARGET_SCHEMA'   -- Target schema name
   );

5. Query results:
   -- Best matches
   SELECT * FROM SEMANTIC_MAPPING.V_BEST_MATCHES;
   
   -- All candidates
   SELECT * FROM SEMANTIC_MAPPING.V_ALL_MATCH_CANDIDATES
   WHERE SOURCE_TABLE = 'MY_TABLE';
   
   -- Unmapped columns
   SELECT * FROM SEMANTIC_MAPPING.V_UNMAPPED_SUMMARY;
   
   -- Statistics
   SELECT * FROM SEMANTIC_MAPPING.V_MAPPING_STATISTICS;

CONFIGURATION OPTIONS:
======================

- MATCHING_METHOD: 'TRADITIONAL' or 'CORTEX_LLM'
- CONFIDENCE_THRESHOLD: Minimum score (0.0 to 1.0)
- TOP_N_MATCHES: Number of candidate matches per source column
- WEIGHT_EXACT_MATCH: Weight for exact name matches (0.0 to 1.0)
- WEIGHT_FUZZY_MATCH: Weight for fuzzy string similarity (0.0 to 1.0)
- WEIGHT_TOKEN_MATCH: Weight for token-based matching (0.0 to 1.0)
- WEIGHT_PHONETIC_MATCH: Weight for phonetic similarity (0.0 to 1.0)
- WEIGHT_SYNONYM_MATCH: Weight for synonym/abbreviation matches (0.0 to 1.0)
- WEIGHT_DATATYPE_MATCH: Weight for compatible data types (0.0 to 1.0)

CUSTOMIZATION:
==============

- Add more abbreviations/synonyms to DICT_ABBREVIATIONS table
- Adjust weights to prioritize certain matching methods
- Add schema/table exclusion patterns
- Modify transformation logic in SUGGEST_TRANSFORMATION function

NOTES:
======

- CORTEX_LLM method requires Snowflake account with Cortex AI features enabled
- Traditional method works on all Snowflake editions
- Results are stored in MAPPING_RESULTS and UNMAPPED_COLUMNS tables
- Multiple executions will overwrite previous results
- All matching is case-insensitive
*/