CREATE TABLE splits (
    split_id VARCHAR(50) PRIMARY KEY,
    num_records BIGINT DEFAULT 0 CHECK(num_records >= 0),
    size_in_bytes BIGINT DEFAULT 0 CHECK(num_records >= 0),
    start_time_range BIGINT,
    end_time_range BIGINT,
    split_state VARCHAR(30) NOT NULL,
    update_timestamp BIGINT DEFAULT 0,
    tags TEXT[] NOT NULL,
    start_footer_offsets BIGINT DEFAULT 0 CHECK(num_records >= 0);
    end_footer_offsets BIGINT DEFAULT 0 CHECK(num_records >= 0);
    index_id VARCHAR(50) NOT NULL,

    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);
