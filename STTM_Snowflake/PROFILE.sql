CREATE OR REPLACE PROCEDURE summarize_table(full_table_name STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
DECLARE
  cols_cursor CURSOR FOR 
    SELECT column_name 
    FROM table(information_schema.columns) 
    WHERE table_catalog || '.' || table_schema || '.' || table_name = UPPER(:full_table_name)
    ORDER BY ordinal_position;
  sql_query STRING DEFAULT 'SELECT ';
  counter INTEGER DEFAULT 0;
  res RESULTSET;
BEGIN
  -- Build the dynamic SQL string
  FOR record IN cols_cursor DO
    IF (counter > 0) THEN
      sql_query := sql_query || ', ';
    END IF;
    
    -- Add summary stats for each column (Count, Nulls, Distinct)
    sql_query := sql_query || 
      'COUNT("' || record.column_name || '") AS "' || record.column_name || '_COUNT", ' ||
      'COUNT(DISTINCT "' || record.column_name || '") AS "' || record.column_name || '_UNIQUE", ' ||
      'COUNT_IF("' || record.column_name || '" IS NULL) AS "' || record.column_name || '_NULLS"';
    
    counter := counter + 1;
  END FOR;

  sql_query := sql_query || ' FROM ' || :full_table_name;
  
  -- Execute the final constructed query
  res := (EXECUTE IMMEDIATE :sql_query);
  RETURN TABLE(res);
END;
