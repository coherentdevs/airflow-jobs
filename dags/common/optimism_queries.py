SQL_CREATE_OPTIMISM_RAW_BLOCKS_TABLE = """
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
SQL_CREATE_OPTIMISM_RAW_TRANSACTIONS_TABLE = """
create or replace table TRANSACTIONS (
    block_hash VARCHAR,
    block_number VARCHAR,
    cumulative_gas_used VARCHAR,
    from_address VARCHAR,
    gas VARCHAR,
    gas_price VARCHAR,
    gas_used VARCHAR,
    input VARCHAR,
    logs_bloom VARCHAR,
    nonce VARCHAR,
    r VARCHAR,
    s VARCHAR,
    status VARCHAR,
    to_address VARCHAR,
    transaction_hash VARCHAR,
    transaction_index VARCHAR,
    v VARCHAR,
    value VARCHAR,
    queue_origin VARCHAR,
    l1_tx_origin VARCHAR,
    l1_block_number VARCHAR,
    l1_timestamp VARCHAR,
    index VARCHAR,
    queue_index VARCHAR,
    raw_transaction VARCHAR,
    l1_fee VARCHAR,
    l1_fee_scalar VARCHAR,
    l1_gas_price VARCHAR,
    l1_gas_used VARCHAR,
    constraint transactions_pkey primary key (transaction_hash) enforced
);
"""
SQL_CREATE_OPTIMISM_RAW_TRACES_TABLE = """
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
    CONSTRAINT traces_pkey PRIMARY KEY (transaction_hash, trace_hash, parent_hash, trace_index) ENFORCED
)
CLUSTER BY (block_number, transaction_hash);
"""
SQL_CREATE_OPTIMISM_RAW_LOGS_TABLE = """
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
)
CLUSTER BY (transaction_hash);
"""

SQL_CREATE_OPTIMISM_RAW_BLOCKS_PIPE="""
CREATE or REPLACE PIPE optimism_raw_blocks
  AUTO_INGEST = true
  INTEGRATION = optimism_raw_notification
  AS
COPY INTO BLOCKS
FROM @optimism_raw_stage/blocks/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_OPTIMISM_RAW_TRANSACTIONS_PIPE="""
CREATE or REPLACE PIPE optimism_raw_transactions
  AUTO_INGEST = true
  INTEGRATION = optimism_raw_notification
  AS
COPY INTO TRANSACTIONS
FROM @optimism_raw_stage/transactions/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_OPTIMISM_RAW_TRACES_PIPE="""
CREATE or REPLACE PIPE optimism_raw_traces
  AUTO_INGEST = true
  INTEGRATION = optimism_raw_notification
  AS
COPY INTO TRACES
FROM @optimism_raw_stage/traces/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_OPTIMISM_RAW_LOGS_PIPE="""
CREATE or REPLACE PIPE optimism_raw_logs
  AUTO_INGEST = true
  INTEGRATION = optimism_raw_notification
  AS
COPY INTO LOGS
FROM @optimism_raw_stage/logs/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

OPTIMISM_COPY_FMT = """
COPY INTO
    {table_name}
FROM
	@optimism_raw_stage/{object_type}/blocks_{start}-{end}/
	    file_format = parquet_format 
	    match_by_column_name = case_insensitive 
	    pattern = '^(.+)\.parquet$'
"""
