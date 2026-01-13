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

-- Whitelist/inclusion list for specific tables
CREATE OR REPLACE TABLE CONFIG_INCLUSIONS (
    INCLUSION_TYPE VARCHAR(20), -- 'TABLE'
    DATABASE_NAME VARCHAR(256),
    SCHEMA_NAME VARCHAR(256),
    TABLE_NAME VARCHAR(256),
    DESCRIPTION VARCHAR(1000),
    CONSTRAINT unique_inclusion UNIQUE (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
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
    if (maxDist < 0) maxDist = 0;
    
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
    
    if (len1 === 0) return len2 === 0 ? 1.0 : 0.0;
    if (len2 === 0) return 0.0;
    
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
        scores.fuzzy = fuzzyRs.getColumnValue(1) || 0;
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
        var srcCnt = tokenRs.getColumnValue(1) || 0;
        var tgtCnt = tokenRs.getColumnValue(2) || 0;
        var matchCnt = tokenRs.getColumnValue(3) || 0;
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
        var srcCnt = synonymRs.getColumnValue(1) || 0;
        var tgtCnt = synonymRs.getColumnValue(2) || 0;
        var matchCnt = synonymRs.getColumnValue(3) || 0;
        if (srcCnt > 0 && tgtCnt > 0) {
            scores.synonym = (2 * matchCnt) / (srcCnt + tgtCnt);
        }
    }
    
    // Data type compatibility
    var typeCompatibility = {
        'NUMBER': ['NUMBER', 'NUMERIC', 'INTEGER', 'FLOAT', 'DECIMAL', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT'],
        'VARCHAR': ['VARCHAR', 'STRING', 'TEXT', 'CHAR', 'CHARACTER'],
        'DATE': ['DATE'],
        'TIMESTAMP': ['TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'DATETIME'],
        'BOOLEAN': ['BOOLEAN', 'BOOL'],
        'BINARY': ['BINARY', 'VARBINARY'],
        'VARIANT': ['VARIANT', 'OBJECT', 'ARRAY']
    };
    
    var srcBase = (SOURCE_TYPE || '').split('(')[0].toUpperCase().trim();
    var tgtBase = (TARGET_TYPE || '').split('(')[0].toUpperCase().trim();
    
    if (srcBase === tgtBase) {
        scores.datatype = 1.0;
    } else {
        // Check compatibility groups
        for (var key in typeCompatibility) {
            var types = typeCompatibility[key];
            var srcInGroup = types.indexOf(srcBase) >= 0;
            var tgtInGroup = types.indexOf(tgtBase) >= 0;
            if (srcInGroup && tgtInGroup) {
                scores.datatype = 0.8;
                break;
            }
        }
        // Special case: DATE and TIMESTAMP are somewhat compatible
        if ((srcBase === 'DATE' && tgtBase.indexOf('TIMESTAMP') >= 0) ||
            (srcBase.indexOf('TIMESTAMP') >= 0 && tgtBase === 'DATE')) {
            scores.datatype = 0.6;
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
    
    var finalScore = totalWeight > 0 ? totalScore / totalWeight : 0;
    
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
    
    var srcType = (SOURCE_TYPE || '').toUpperCase().split('(')[0].trim();
    var tgtType = (TARGET_TYPE || '').toUpperCase().split('(')[0].trim();
    
    var srcCol = SOURCE_COL;
    var currentCol = srcCol;
    
    // Data type conversion
    if (srcType !== tgtType) {
        if (tgtType === 'NUMBER' || tgtType === 'NUMERIC' || tgtType === 'INTEGER' || 
            tgtType === 'INT' || tgtType === 'BIGINT' || tgtType === 'FLOAT' || tgtType === 'DECIMAL') {
            currentCol = 'TRY_CAST(' + currentCol + ' AS NUMBER)';
            transformations.push('Type: ' + srcType + '->NUMBER');
        } else if (tgtType === 'DATE') {
            currentCol = 'TRY_TO_DATE(' + currentCol + ')';
            transformations.push('Type: ' + srcType + '->DATE');
        } else if (tgtType.indexOf('TIMESTAMP') >= 0 || tgtType === 'DATETIME') {
            currentCol = 'TRY_TO_TIMESTAMP(' + currentCol + ')';
            transformations.push('Type: ' + srcType + '->TIMESTAMP');
        } else if (tgtType === 'BOOLEAN' || tgtType === 'BOOL') {
            currentCol = 'TRY_TO_BOOLEAN(' + currentCol + ')';
            transformations.push('Type: ' + srcType + '->BOOLEAN');
        } else if (tgtType === 'VARCHAR' || tgtType === 'STRING' || tgtType === 'TEXT') {
            currentCol = 'TO_VARCHAR(' + currentCol + ')';
            transformations.push('Type: ' + srcType + '->VARCHAR');
        }
    }
    
    // Case handling for string types
    if ((srcType === 'VARCHAR' || srcType === 'STRING' || srcType === 'TEXT' || srcType === 'CHAR') && 
        (tgtType === 'VARCHAR' || tgtType === 'STRING' || tgtType === 'TEXT' || tgtType === 'CHAR')) {
        
        // Trim whitespace
        currentCol = 'TRIM(' + currentCol + ')';
        transformations.push('String: TRIM');
        
        // Analyze column name patterns for case conversion hints
        var srcUpper = SOURCE_COL === SOURCE_COL.toUpperCase();
        var tgtUpper = TARGET_COL === TARGET_COL.toUpperCase();
        var srcLower = SOURCE_COL === SOURCE_COL.toLowerCase();
        var tgtLower = TARGET_COL === TARGET_COL.toLowerCase();
        
        if (!srcUpper && tgtUpper) {
            currentCol = 'UPPER(' + currentCol + ')';
            transformations.push('Case: UPPER');
        } else if (!srcLower && tgtLower) {
            currentCol = 'LOWER(' + currentCol + ')';
            transformations.push('Case: LOWER');
        }
    }
    
    // Null handling - always add COALESCE
    currentCol = 'COALESCE(' + currentCol + ', NULL)';
    transformations.push('Null: COALESCE');
    
    var comment = transformations.length > 0 ? ' -- ' + transformations.join(', ') : '';
    return currentCol + ' AS ' + TARGET_COL + comment;
$$;

-- =====================================================================
-- MAIN MAPPING PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE SEMANTIC_MAPPING.EXECUTE_SEMANTIC_MAPPING(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR,
    TARGET_DATABASE VARCHAR,
    TARGET_SCHEMA VARCHAR,
    OUTPUT_SCHEMA VARCHAR DEFAULT 'SEMANTIC_MAPPING'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // 1. Fetch Configuration
        var configRs = snowflake.execute({
            sqlText: `SELECT PARAMETER_NAME, PARAMETER_VALUE FROM ${OUTPUT_SCHEMA}.CONFIG_MATCHING_PARAMETERS`
        });
        var config = {};
        while (configRs.next()) {
            config[configRs.getColumnValue(1)] = configRs.getColumnValue(2);
        }
        
        var method = config['MATCHING_METHOD'] || 'TRADITIONAL';
        var threshold = parseFloat(config['CONFIDENCE_THRESHOLD'] || '0.6');
        var topN = parseInt(config['TOP_N_MATCHES'] || '5');

        // 2. Cleanup Previous Results
        snowflake.execute({sqlText: `TRUNCATE TABLE ${OUTPUT_SCHEMA}.MAPPING_RESULTS`});
        snowflake.execute({sqlText: `TRUNCATE TABLE ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS`});

        // 3. Materialize Metadata (Caching for Performance)
        // Check if whitelist has entries
        var whitelistCheckRs = snowflake.execute({
            sqlText: `SELECT COUNT(*) as cnt FROM ${OUTPUT_SCHEMA}.CONFIG_INCLUSIONS 
                      WHERE DATABASE_NAME = '${SOURCE_DATABASE}' AND SCHEMA_NAME = '${SOURCE_SCHEMA}'`
        });

        whitelistCheckRs.next();
        var hasWhitelist = whitelistCheckRs.getColumnValue(1) > 0;
        // Build source columns query with optional whitelist filter
        var sourceColsSql = `CREATE OR REPLACE TEMPORARY TABLE temp_source_cols AS 
                             SELECT TABLE_CATALOG as db, TABLE_SCHEMA as schema, TABLE_NAME as tbl, 
                                    COLUMN_NAME as col, DATA_TYPE as dtype 
                             FROM ${SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS 
                             WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}'`;
        
        if (hasWhitelist) {
            sourceColsSql += ` AND TABLE_NAME IN (
                SELECT TABLE_NAME FROM ${OUTPUT_SCHEMA}.CONFIG_INCLUSIONS 
                WHERE DATABASE_NAME = '${SOURCE_DATABASE}' 
                AND SCHEMA_NAME = '${SOURCE_SCHEMA}')`;
        }
        
        snowflake.execute({sqlText: sourceColsSql});
        
        snowflake.execute({
            sqlText: `CREATE OR REPLACE TEMPORARY TABLE temp_target_cols AS 
                      SELECT TABLE_CATALOG as db, TABLE_SCHEMA as schema, TABLE_NAME as tbl, 
                             COLUMN_NAME as col, DATA_TYPE as dtype 
                      FROM ${TARGET_DATABASE}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${TARGET_SCHEMA}'`
        });

        // 4. Execute Context-Aware Mapping
        var mappingSql = "";
        
        if (method === 'CORTEX_LLM') {
            // Cortex Path: Tables and columns are embedded as a single semantic unit
            mappingSql = `
                INSERT INTO ${OUTPUT_SCHEMA}.MAPPING_RESULTS (
                    SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, SOURCE_DATA_TYPE,
                    TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TARGET_DATA_TYPE,
                    CONFIDENCE_SCORE, MATCH_RANK, TRANSFORMATION_SQL, MATCHING_METHOD, MATCH_DETAILS
                )
                WITH src_emb AS (
                    SELECT *, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', tbl || ' ' || col) as emb FROM temp_source_cols
                ),
                tgt_emb AS (
                    SELECT *, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', tbl || ' ' || col) as emb FROM temp_target_cols
                ),
                scored AS (
                    SELECT s.db, s.schema, s.tbl, s.col, s.dtype, 
                           t.db as t_db, t.schema as t_schema, t.tbl as t_tbl, t.col as t_col, t.dtype as t_type,
                           VECTOR_COSINE_SIMILARITY(s.emb, t.emb) as score,
                           -- Partition by target instead of source - find fit to target
           ROW_NUMBER() OVER (PARTITION BY t.db, t.schema, t.tbl, t.col ORDER BY score DESC) as rn
    FROM src_emb s CROSS JOIN tgt_emb t
                )
                SELECT db, schema, tbl, col, dtype, t_db, t_schema, t_tbl, t_col, t_type, 
                       score, rn, ${OUTPUT_SCHEMA}.SUGGEST_TRANSFORMATION(col, dtype, t_col, t_type), 
                       'CORTEX_LLM_CONTEXTUAL', OBJECT_CONSTRUCT('context', 'table_plus_column')
                FROM scored WHERE score >= ${threshold} AND rn <= ${topN}`;
        } else {
            // Traditional Path: Fuzzy logic compares concatenated strings
            mappingSql = `
                INSERT INTO ${OUTPUT_SCHEMA}.MAPPING_RESULTS (
                    SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, SOURCE_DATA_TYPE,
                    TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TARGET_DATA_TYPE,
                    CONFIDENCE_SCORE, MATCH_RANK, TRANSFORMATION_SQL, MATCHING_METHOD, MATCH_DETAILS
                )
                WITH weights AS (
                    SELECT 
                        MAX(CASE WHEN PARAMETER_NAME = 'WEIGHT_EXACT_MATCH' THEN PARAMETER_VALUE::FLOAT END) as w_exact,
                        MAX(CASE WHEN PARAMETER_NAME = 'WEIGHT_FUZZY_MATCH' THEN PARAMETER_VALUE::FLOAT END) as w_fuzzy,
                        MAX(CASE WHEN PARAMETER_NAME = 'WEIGHT_PHONETIC_MATCH' THEN PARAMETER_VALUE::FLOAT END) as w_phonetic,
                        MAX(CASE WHEN PARAMETER_NAME = 'WEIGHT_DATATYPE_MATCH' THEN PARAMETER_VALUE::FLOAT END) as w_datatype
                    FROM ${OUTPUT_SCHEMA}.CONFIG_MATCHING_PARAMETERS
                ),
                scored AS (
                    SELECT 
                        s.db as s_db, s.schema as s_schema, s.tbl as s_tbl, s.col as s_col, s.dtype as s_type,
                        t.db as t_db, t.schema as t_schema, t.tbl as t_tbl, t.col as t_col, t.dtype as t_type,
                        ( (CASE WHEN UPPER(s.col) = UPPER(t.col) THEN 1.0 ELSE 0.0 END * w.w_exact) +
                          (JAROWINKLER_SIMILARITY(s.tbl || '_' || s.col, t.tbl || '_' || t.col)/100.0 * w.w_fuzzy) +
                          (CASE WHEN SOUNDEX(s.col) = SOUNDEX(t.col) THEN 1.0 ELSE 0.0 END * w.w_phonetic) +
                          (CASE WHEN SPLIT_PART(s.dtype, '(', 1) = SPLIT_PART(t.dtype, '(', 1) THEN 1.0 ELSE 0.2 END * w.w_datatype)
                        ) / NULLIF(w.w_exact + w.w_fuzzy + w.w_phonetic + w.w_datatype, 0) as score,
                        -- Partition by target instead of source - get best target match
        ROW_NUMBER() OVER (PARTITION BY t_db, t_schema, t_tbl, t_col ORDER BY score DESC) as rn
    FROM temp_source_cols s CROSS JOIN temp_target_cols t CROSS JOIN weights w
                )
                SELECT s_db, s_schema, s_tbl, s_col, s_type, t_db, t_schema, t_tbl, t_col, t_type,
                       score, rn, ${OUTPUT_SCHEMA}.SUGGEST_TRANSFORMATION(s_col, s_type, t_col, t_type),
                       'TRADITIONAL_CONTEXTUAL', OBJECT_CONSTRUCT('logic', 'table_qualified_similarity')
                FROM scored WHERE score >= ${threshold} AND rn <= ${topN}`;
        }

        snowflake.execute({sqlText: mappingSql});

        // 5. Unmapped Tracking
        snowflake.execute({
            sqlText: `INSERT INTO ${OUTPUT_SCHEMA}.UNMAPPED_COLUMNS (SIDE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE)
                      SELECT 'SOURCE', db, schema, tbl, col, dtype FROM temp_source_cols s
                      WHERE NOT EXISTS (SELECT 1 FROM ${OUTPUT_SCHEMA}.MAPPING_RESULTS m 
                                        WHERE m.SOURCE_COLUMN = s.col AND m.MATCH_RANK = 1)`
        });

        var summary = snowflake.execute({
            sqlText: `SELECT COUNT(*) FROM ${OUTPUT_SCHEMA}.MAPPING_RESULTS WHERE MATCH_RANK = 1`
        });
        summary.next();
        return `Success: Mapped ${summary.getColumnValue(1)} columns using ${method} (Table-Aware) method.`;

    } catch (err) {
        return "Error: " + err.message;
    }
$$;

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
    ROUND(AVG(CONFIDENCE_SCORE), 4) AS avg_confidence,
    ROUND(MIN(CONFIDENCE_SCORE), 4) AS min_confidence,
    ROUND(MAX(CONFIDENCE_SCORE), 4) AS max_confidence,
    COUNT(*) AS total_mappings
FROM MAPPING_RESULTS
WHERE MATCH_RANK = 1
GROUP BY MATCHING_METHOD;

-- View: Detailed match breakdown by component scores
CREATE OR REPLACE VIEW V_MATCH_SCORE_BREAKDOWN AS
SELECT 
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    TARGET_TABLE,
    TARGET_COLUMN,
    CONFIDENCE_SCORE,
    MATCH_RANK,
    MATCH_DETAILS:component_scores:exact::FLOAT AS exact_score,
    MATCH_DETAILS:component_scores:fuzzy::FLOAT AS fuzzy_score,
    MATCH_DETAILS:component_scores:token::FLOAT AS token_score,
    MATCH_DETAILS:component_scores:phonetic::FLOAT AS phonetic_score,
    MATCH_DETAILS:component_scores:synonym::FLOAT AS synonym_score,
    MATCH_DETAILS:component_scores:datatype::FLOAT AS datatype_score
FROM MAPPING_RESULTS
WHERE MATCHING_METHOD = 'TRADITIONAL'
ORDER BY SOURCE_TABLE, SOURCE_COLUMN, MATCH_RANK;

-- View: Whitelist configuration status
CREATE OR REPLACE VIEW V_WHITELIST_STATUS AS
SELECT 
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    DESCRIPTION,
    'INCLUDED' AS STATUS
FROM CONFIG_INCLUSIONS
ORDER BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME;

-- =====================================================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- =====================================================================

/*
SETUP INSTRUCTIONS:
===================

1. Execute this entire script to create the schema, tables, functions, procedures, and views.

2. Configure matching parameters (optional - defaults are provided):
   
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = 'CORTEX_LLM'  -- or 'TRADITIONAL'
   WHERE PARAMETER_NAME = 'MATCHING_METHOD';
   
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = '0.70'
   WHERE PARAMETER_NAME = 'CONFIDENCE_THRESHOLD';
   
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = '3'
   WHERE PARAMETER_NAME = 'TOP_N_MATCHES';
   
   -- Adjust weights (optional)
   UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
   SET PARAMETER_VALUE = '1.0'
   WHERE PARAMETER_NAME = 'WEIGHT_SYNONYM_MATCH';

3. (Optional) Add exclusions:
   
   INSERT INTO SEMANTIC_MAPPING.CONFIG_EXCLUSIONS VALUES
   ('SCHEMA', 'INFORMATION_SCHEMA', 'Exclude system schemas'),
   ('TABLE', '%_BACKUP', 'Exclude backup tables');

3.5 (Optional) Add tables to whitelist for selective processing:
   
   -- Only process specific tables from source schema
   INSERT INTO SEMANTIC_MAPPING.CONFIG_INCLUSIONS VALUES
   ('TABLE', 'SOURCE_DB', 'SOURCE_SCHEMA', 'CUSTOMERS', 'Include customer table'),
   ('TABLE', 'SOURCE_DB', 'SOURCE_SCHEMA', 'ORDERS', 'Include orders table'),
   ('TABLE', 'SOURCE_DB', 'SOURCE_SCHEMA', 'PRODUCTS', 'Include products table');
   
   -- View current whitelist
   SELECT * FROM SEMANTIC_MAPPING.CONFIG_INCLUSIONS;
   
   -- Clear whitelist to process all tables
   DELETE FROM SEMANTIC_MAPPING.CONFIG_INCLUSIONS 
   WHERE DATABASE_NAME = 'SOURCE_DB' AND SCHEMA_NAME = 'SOURCE_SCHEMA';

4. (Optional) Add custom abbreviations/synonyms:
   
   INSERT INTO SEMANTIC_MAPPING.DICT_ABBREVIATIONS VALUES
   ('CUST_NO', 'CUSTOMER_NUMBER', 'BUSINESS'),
   ('ORG', 'ORGANIZATION', 'BUSINESS');

5. Execute the mapping:
   
   CALL SEMANTIC_MAPPING.EXECUTE_SEMANTIC_MAPPING(
       'SOURCE_DB',      -- Source database name
       'SOURCE_SCHEMA',  -- Source schema name
       'TARGET_DB',      -- Target database name
       'TARGET_SCHEMA'   -- Target schema name
   );

6. Query results:
   
   -- Best matches only (rank 1)
   SELECT * FROM SEMANTIC_MAPPING.V_BEST_MATCHES;
   
   -- All candidate matches with rankings
   SELECT * FROM SEMANTIC_MAPPING.V_ALL_MATCH_CANDIDATES
   WHERE SOURCE_TABLE = 'MY_TABLE';
   
   -- Unmapped columns
   SELECT * FROM SEMANTIC_MAPPING.V_UNMAPPED_SUMMARY;
   
   -- Overall statistics
   SELECT * FROM SEMANTIC_MAPPING.V_MAPPING_STATISTICS;
   
   -- Detailed score breakdown (Traditional method only)
   SELECT * FROM SEMANTIC_MAPPING.V_MATCH_SCORE_BREAKDOWN
   WHERE SOURCE_TABLE = 'CUSTOMERS';

CONFIGURATION OPTIONS:
======================

- MATCHING_METHOD: 'TRADITIONAL' or 'CORTEX_LLM'
  * TRADITIONAL: Uses string similarity, tokens, phonetics, synonyms (works on all editions)
  * CORTEX_LLM: Uses AI embeddings for semantic matching (requires Cortex AI features)

- CONFIDENCE_THRESHOLD: Minimum score (0.0 to 1.0, default 0.60)
  * Lower values = more matches but lower quality
  * Higher values = fewer matches but higher quality

- TOP_N_MATCHES: Number of candidate matches per source column (default 5)
  * Useful for reviewing alternative mappings

- Weights (all default to values between 0.0 and 1.0):
  * WEIGHT_EXACT_MATCH: Exact column name matches (default 1.0)
  * WEIGHT_FUZZY_MATCH: String similarity (default 0.7)
  * WEIGHT_TOKEN_MATCH: Word/token matching (default 0.8)
  * WEIGHT_PHONETIC_MATCH: Sound-alike names (default 0.5)
  * WEIGHT_SYNONYM_MATCH: Abbreviation expansion (default 0.9)
  * WEIGHT_DATATYPE_MATCH: Compatible data types (default 0.3)

PERFORMANCE NOTES:
==================

- The Traditional method processes all source x target column combinations
- For large schemas (100s of tables, 1000s of columns), execution may take several minutes
- The Cortex LLM method is typically faster but requires appropriate Snowflake features
- Consider filtering to specific tables if only partial mapping is needed
- Results are stored in tables, so you only need to run the mapping once

CUSTOMIZATION:
==============

- Add industry-specific abbreviations to DICT_ABBREVIATIONS
- Adjust weights to prioritize certain matching aspects for your domain
- Add schema/table exclusion patterns for system tables
- Modify SUGGEST_TRANSFORMATION function for custom transformation logic
- Extend CALCULATE_TRADITIONAL_MATCH for additional scoring components

TROUBLESHOOTING:
================

- "No source/target columns found": Check database and schema names are correct
- "Cortex LLM error": Ensure your Snowflake account has Cortex AI enabled
- Low confidence scores: Try adjusting weights or lowering threshold
- Too many matches: Increase confidence threshold or reduce TOP_N_MATCHES
- Missing abbreviations: Add custom entries to DICT_ABBREVIATIONS table

EXAMPLE WORKFLOW:
=================

-- 1. Set to Traditional matching with moderate threshold
UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
SET PARAMETER_VALUE = 'TRADITIONAL' WHERE PARAMETER_NAME = 'MATCHING_METHOD';

UPDATE SEMANTIC_MAPPING.CONFIG_MATCHING_PARAMETERS 
SET PARAMETER_VALUE = '0.65' WHERE PARAMETER_NAME = 'CONFIDENCE_THRESHOLD';

-- 2. Run the mapping
CALL SEMANTIC_MAPPING.EXECUTE_SEMANTIC_MAPPING('MY_SOURCE_DB', 'PUBLIC', 'MY_TARGET_DB', 'PUBLIC');

-- 3. Review best matches
SELECT 
    SOURCE_TABLE,
    SOURCE_COLUMN,
    TARGET_TABLE,
    TARGET_COLUMN,
    ROUND(CONFIDENCE_SCORE, 3) AS SCORE,
    TRANSFORMATION_SQL
FROM SEMANTIC_MAPPING.V_BEST_MATCHES
WHERE CONFIDENCE_SCORE >= 0.8
ORDER BY SOURCE_TABLE, SOURCE_COLUMN;

-- 4. Check unmapped columns
SELECT SIDE, TABLE_NAME, COUNT(*) AS unmapped_count
FROM SEMANTIC_MAPPING.V_UNMAPPED_SUMMARY
GROUP BY SIDE, TABLE_NAME
ORDER BY unmapped_count DESC;

-- 5. Review alternative matches for specific columns
SELECT 
    TARGET_COLUMN,
    CONFIDENCE_SCORE,
    MATCH_RANK,
    TRANSFORMATION_SQL
FROM SEMANTIC_MAPPING.V_ALL_MATCH_CANDIDATES
WHERE SOURCE_TABLE = 'ORDERS' AND SOURCE_COLUMN = 'CUST_ID'
ORDER BY MATCH_RANK;
*/