SQL_CREATE_TESTNET_BASE_RAW_BLOCKS_TABLE = """
CREATE OR REPLACE TABLE blocks (
    block_number VARCHAR,
    block_hash VARCHAR,
    parent_hash VARCHAR,
    nonce VARCHAR,
    sha3_uncles VARCHAR,
    logs_bloom VARCHAR,
    transactions_root VARCHAR,
    state_root VARCHAR,
    receipts_root VARCHAR,
    miner VARCHAR,
    difficulty VARCHAR,
    total_difficulty VARCHAR,
    extra_data VARCHAR,
    size VARCHAR,
    gas_limit VARCHAR,
    gas_used VARCHAR,
    timestamp VARCHAR,
    uncles VARCHAR,
    base_fee_per_gas VARCHAR,
    mix_hash VARCHAR,
    CONSTRAINT blocks_pkey PRIMARY KEY (block_number) ENFORCED
);
"""

SQL_CREATE_TESTNET_BASE_RAW_TRANSACTIONS_TABLE = """
CREATE OR REPLACE TABLE transactions (
    block_number VARCHAR,
    block_hash VARCHAR,
    transaction_hash VARCHAR,
    from_address VARCHAR,
    to_address VARCHAR,
    value VARCHAR,
    gas VARCHAR,
    gas_price VARCHAR,
    input VARCHAR,
    nonce VARCHAR,
    transaction_index VARCHAR,
    v VARCHAR,
    r VARCHAR,
    s VARCHAR,
    source_hash VARCHAR,
    mint VARCHAR,
    is_system_tx BOOLEAN,
    cumulative_gas_used VARCHAR,
    effective_gas_price VARCHAR,
    gas_used VARCHAR,
    logs_bloom VARCHAR,
    status VARCHAR,
    l1_fee VARCHAR,
    l1_fee_scalar VARCHAR,
    l1_gas_price VARCHAR,
    l1_gas_used VARCHAR,
    type VARCHAR,
    max_fee_per_gas VARCHAR,
    max_priority_fee_per_gas VARCHAR,
    access_list VARCHAR,
    CONSTRAINT transactions_pkey PRIMARY KEY (transaction_hash) ENFORCED
);
"""

SQL_CREATE_TESTNET_BASE_RAW_LOGS_TABLE = """
CREATE OR REPLACE TABLE logs (
    block_number VARCHAR,
    block_hash VARCHAR,
    transaction_hash VARCHAR,
    transaction_index VARCHAR,
    log_index VARCHAR,
    address VARCHAR,
    data VARCHAR,
    topics VARCHAR,
    removed BOOLEAN,
    CONSTRAINT logs_pkey PRIMARY KEY (transaction_hash, log_index) ENFORCED
)
CLUSTER BY (transaction_hash);
"""

SQL_CREATE_TESTNET_BASE_RAW_TRACES_TABLE = """
CREATE OR REPLACE TABLE traces (
    block_number VARCHAR,
    block_hash VARCHAR,
    transaction_hash VARCHAR,
    trace_hash VARCHAR,
    parent_hash VARCHAR,
    trace_index VARCHAR,
    type VARCHAR,
    from_address VARCHAR,
    to_address VARCHAR,
    value VARCHAR,
    gas VARCHAR,
    gas_used VARCHAR,
    input VARCHAR,
    output VARCHAR,
    error VARCHAR,
    revert_reason VARCHAR,
    CONSTRAINT traces_pkey PRIMARY KEY (transaction_hash, trace_hash, parent_hash, trace_index) ENFORCED
)
CLUSTER BY (block_number, transaction_hash);
;
"""

SQL_CREATE_TESTNET_BASE_RAW_BLOCKS_PIPE="""
CREATE or REPLACE PIPE testnet_base_raw_blocks
  AUTO_INGEST = true
  INTEGRATION = testnet_base_raw_notification
  AS
COPY INTO BLOCKS
FROM @testnet_base_raw_stage/blocks/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_TESTNET_BASE_RAW_TRANSACTIONS_PIPE="""
CREATE or REPLACE PIPE testnet_base_raw_transactions
  AUTO_INGEST = true
  INTEGRATION = testnet_base_raw_notification
  AS
COPY INTO TRANSACTIONS
FROM @testnet_base_raw_stage/transactions/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_TESTNET_BASE_RAW_TRACES_PIPE="""
CREATE or REPLACE PIPE testnet_base_raw_traces
  AUTO_INGEST = true
  INTEGRATION = testnet_base_raw_notification
  AS
COPY INTO TRACES
FROM @testnet_base_raw_stage/traces/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

SQL_CREATE_TESTNET_BASE_RAW_LOGS_PIPE="""
CREATE or REPLACE PIPE testnet_base_raw_logs
  AUTO_INGEST = true
  INTEGRATION = testnet_base_raw_notification
  AS
COPY INTO LOGS
FROM @testnet_base_raw_stage/logs/ file_format = parquet_format match_by_column_name = case_insensitive pattern = '(.+)\.parquet';
"""

TESTNET_BASE_COPY_FMT = """
COPY INTO
    {table_name}
FROM
	@testnet_base_raw_stage/{object_type}/blocks_{start}-{end}/
	    file_format = parquet_format 
	    match_by_column_name = case_insensitive 
	    pattern = '^(.+)\.parquet$'
"""