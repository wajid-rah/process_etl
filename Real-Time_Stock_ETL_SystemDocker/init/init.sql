    CREATE TABLE IF NOT EXISTS stock_daily (
        symbol           VARCHAR(10)    NOT NULL,
        date             DATE           NOT NULL,
        open             DECIMAL(10, 4),
        high             DECIMAL(10, 4),
        low              DECIMAL(10, 4),
        close            DECIMAL(10, 4),
        volume           BIGINT,
        daily_range      DECIMAL(10, 4),
        daily_return_pct DECIMAL(10, 4),
        candle           VARCHAR(10),
        PRIMARY KEY (symbol, date)
    );