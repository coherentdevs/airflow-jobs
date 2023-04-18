SQL_CREATE_ETH_RAW_BLOCKS_TABLE="""
CREATE OR REPLACE TABLE BLOCKS (
    difficulty VARCHAR,
    extra_data VARCHAR,
    gas_limit VARCHAR,
    gas_used VARCHAR,
    block_hash VARCHAR,
    block_number VARCHAR,
    logs_bloom VARCHAR,
    miner VARCHAR,
    mix_hash VARCHAR,
    nonce VARCHAR,
    parent_hash VARCHAR,
    receipts_root VARCHAR,
    sha3_uncles VARCHAR,
    size VARCHAR,
    state_root VARCHAR,
    timestamp VARCHAR,
    total_difficulty VARCHAR,
    transactions_root VARCHAR,
    uncles VARCHAR,
    CONSTRAINT blocks_pkey PRIMARY KEY (block_number) ENFORCED
);
"""
SQL_CREATE_ETH_RAW_TRANSACTIONS_TABLE="""
create or replace table TRANSACTIONS (
    block_hash VARCHAR,
    block_number VARCHAR,
    cumulative_gas_used VARCHAR,
    effective_gas_price VARCHAR,
    from_address VARCHAR,
    gas VARCHAR,
    gas_price VARCHAR,
    gas_used VARCHAR,
    input VARCHAR,
    logs_bloom VARCHAR,
    max_fee_per_gas VARCHAR,
    max_priority_fee_per_gas VARCHAR,
    nonce VARCHAR,
    r VARCHAR,
    s VARCHAR,
    status VARCHAR,
    to_address VARCHAR,
    transaction_hash VARCHAR,
    transaction_index VARCHAR,
    type VARCHAR,
    v VARCHAR,
    value VARCHAR,
    access_list VARCHAR,
    constraint transactions_pkey primary key (transaction_hash) enforced
);
"""
SQL_CREATE_ETH_RAW_TRACES_TABLE="""
CREATE OR REPLACE TABLE TRACES (
    block_hash VARCHAR,
    block_number VARCHAR,
    error VARCHAR,
    from_address VARCHAR,
    gas VARCHAR,
    gas_used VARCHAR,
    trace_index VARCHAR,
    input VARCHAR,
    output VARCHAR,
    parent_hash VARCHAR,
    revert_reason VARCHAR,
    to_address VARCHAR,
    trace_hash VARCHAR,
    transaction_hash VARCHAR,
    type VARCHAR,
    value VARCHAR,
    CONSTRAINT traces_pkey PRIMARY KEY (trace_hash) ENFORCED
);
"""
SQL_CREATE_ETH_RAW_LOGS_TABLE="""
create or replace table LOGS (
    address VARCHAR,
    block_hash VARCHAR,
    block_number VARCHAR,
    data VARCHAR,
    log_index VARCHAR,
    topics VARCHAR,
    transaction_hash VARCHAR,
    transaction_index VARCHAR,
    removed BOOLEAN,
    constraint logs_pkey primary key (transaction_hash, log_index) enforced
);
"""

ETH_COPY_FMT = """
COPY INTO
    {table_name}
FROM
	@eth_raw_stage/{object_type}/blocks_{start}-{end}/
	    file_format = parquet_format 
	    match_by_column_name = case_insensitive 
	    pattern = '^(.+)\.parquet$'
"""

SQL_CREATE_ETH_RAW_BLOCKS_PIPE="""
CREATE or REPLACE PIPE eth_raw_blocks
  AUTO_INGEST = true
  INTEGRATION = eth_raw_notification
  AS
COPY INTO BLOCKS
FROM @eth_raw_stage/blocks/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_ETH_RAW_TRANSACTIONS_PIPE="""
CREATE or REPLACE PIPE eth_raw_transactions
  AUTO_INGEST = true
  INTEGRATION = eth_raw_notification
  AS
COPY INTO TRANSACTIONS
FROM @eth_raw_stage/transactions/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_ETH_RAW_TRACES_PIPE="""
CREATE or REPLACE PIPE eth_raw_traces
  AUTO_INGEST = true
  INTEGRATION = eth_raw_notification
  AS
COPY INTO TRACES
FROM @eth_raw_stage/traces/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""
SQL_CREATE_ETH_RAW_LOGS_PIPE="""
CREATE or REPLACE PIPE eth_raw_logs
  AUTO_INGEST = true
  INTEGRATION = eth_raw_notification
  AS
COPY INTO LOGS
FROM @eth_raw_stage/logs/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""