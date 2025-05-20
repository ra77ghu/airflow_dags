-- Sequence creation (if not already created)
CREATE SEQUENCE exchange_id_seq START 1;
CREATE SEQUENCE market_segment_id_seq START 1;

-- Exchange table
CREATE TABLE exchange (
    id INT PRIMARY KEY DEFAULT nextval('exchange_id_seq'),
    exchange VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Market Segment table
CREATE TABLE market_segment (
    id INT PRIMARY KEY DEFAULT nextval('market_segment_id_seq'),
    market_segment VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Formatted Bhavcopy table
CREATE TABLE formatted_bhavcopy (
    trading_session DATE NOT NULL,
    instrument INT NOT NULL,
    expiry_date DATE, --Due to CM contracts doesnt expiry
    close_price NUMERIC(20,8),
    settlement_price NUMERIC(20,8),
    last_price NUMERIC(20,8),
    exchange SMALLINT NOT NULL REFERENCES exchange(id),
    market_segment SMALLINT NOT NULL REFERENCES market_segment(id),
    PRIMARY KEY (trading_session, instrument, exchange, market_segment)
);
