POLYGON_COPY_FMT = """
COPY INTO
    {table_name}
FROM
	@polygon_raw_stage/{object_type}/blocks_{start}-{end}/
	    file_format = parquet_format 
	    force = True
	    match_by_column_name = case_insensitive 
	    pattern = '^(.+)\.parquet$'
"""