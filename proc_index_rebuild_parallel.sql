

-- Author - Kishan 
-- Version 1

CREATE OR REPLACE PROCEDURE rbdidx (
    p_table_names IN VARCHAR2 -- Parameter to pass a comma-separated list of table names (optional)
) 
IS
  V_LEN NUMBER; -- Variable to hold the count from the query
  DOP NUMBER;   -- Variable to hold the degree of parallelism
  CURSOR xcur IS 
    SELECT ai.owner, ai.index_name, at.table_name
    FROM all_indexes ai
    JOIN all_tables at 
      ON ai.table_name = at.table_name
     AND ai.owner = at.owner
    WHERE at.table_name IN (
        SELECT REGEXP_SUBSTR(p_table_names, '[^,]+', 1, LEVEL)
        FROM dual 
        CONNECT BY REGEXP_SUBSTR(p_table_names, '[^,]+', 1, LEVEL) IS NOT NULL
    );
BEGIN
  -- Loop through the selected tables and rebuild their indexes
  FOR rec IN xcur LOOP
    -- Retrieve the value of V_LEN (number of partitions) from all_segments
    BEGIN
      -- Ensure proper segment type for index partitions
      SELECT COUNT(1)
      INTO V_LEN
      FROM dba_segments
      WHERE SEGMENT_NAME = rec.index_name
      AND SEGMENT_TYPE = 'INDEX PARTITION';  -- Ensure correct type for index partitions

      -- If V_LEN is 0 (i.e., no partitions), rebuild indexes for the table
      IF V_LEN = 0 THEN
        -- Calculate the index size in GB
        BEGIN
          -- Ensure correct segment reference for index bytes calculation
          SELECT SUM(bytes) / 1073741824  -- Convert bytes to GB
          INTO DOP
          FROM dba_segments
          WHERE segment_name = rec.index_name;

          -- Set the degree of parallelism to a maximum of 20, or 1 if size <= 1 GB
          IF DOP <= 1 THEN
            DOP := 1;  -- Use DOP = 1 for indexes <= 1 GB
          ELSE
            DOP := LEAST(ROUND(DOP), 20);  -- Limit DOP to 20
          END IF;

          -- Rebuild the index with the calculated degree of parallelism
          EXECUTE IMMEDIATE 'ALTER INDEX ' || rec.owner || '.' || rec.index_name || ' REBUILD PARALLEL ' || DOP;
        END;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        -- Optional: Handle errors if needed, or just log/continue
        NULL;  -- Continue loop if there are errors
    END;
  END LOOP;
END rbdidx;
/

EXEC rbdidx('SP,TP');
