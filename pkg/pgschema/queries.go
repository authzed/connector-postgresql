package pgschema

const (
	querySelectTables = `
SELECT c.OID,s.table_name
FROM   information_schema.Tables s
JOIN   pg_class c ON s.table_name=c.relname
WHERE  s.table_schema != 'information_schema' 
AND    s.table_schema != 'pg_catalog';
`
	querySelectColIds = `
SELECT DISTINCT attname,attnum 
FROM  pg_attribute,pg_class 
WHERE attrelid = pg_class.oid
AND   pg_class.relname=$1;
`
	querySelectPrimaryKeys = `
SELECT a.attnum,a.attname 
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum   = ANY(i.indkey)
WHERE  i.indrelid = $1::regclass
AND    i.indisprimary
`
	querySelectForeignKeys = `
SELECT   string_agg(kcu.column_name, ',') AS fk_columns,
		 string_agg(a.attnum::text, ',') AS fk_column_nums,
         kcu.constraint_name AS constraint_name,
         kcu.table_name AS foreign_table,
         rel_tco.table_name AS primary_table
FROM     information_schema.table_constraints tco
JOIN     information_schema.key_column_usage kcu
            ON tco.constraint_schema = kcu.constraint_schema
           AND tco.constraint_name   = kcu.constraint_name
JOIN     information_schema.referential_constraints rco
            ON tco.constraint_schema = rco.constraint_schema
           AND tco.constraint_name   = rco.constraint_name
JOIN     information_schema.table_constraints rel_tco
            ON rco.unique_constraint_schema = rel_tco.constraint_schema
           AND rco.unique_constraint_name   = rel_tco.constraint_name
JOIN     pg_class c 
           ON c.relname = kcu.table_name
JOIN     pg_attribute a 
           ON a.attrelid = c.OID 
           AND a.attname = kcu.column_name
WHERE    tco.constraint_type = 'FOREIGN KEY'
GROUP BY kcu.table_schema,
         kcu.table_name,
         rel_tco.table_name,
         rel_tco.table_schema,
         kcu.constraint_name
ORDER BY kcu.table_schema,
         kcu.table_name;
`
)
