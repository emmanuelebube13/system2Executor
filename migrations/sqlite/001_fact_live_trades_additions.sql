-- EXEC-006 Fact_Live_Trades additions (SQLite / dev). Additive, nullable, idempotent-ish.
-- SQLite lacks "ADD COLUMN IF NOT EXISTS"; the migration runner guards re-application via the
-- schema_migrations ledger, so this file runs exactly once per database.
CREATE TABLE IF NOT EXISTS Fact_Live_Trades (
    Order_ID            TEXT PRIMARY KEY,
    Correlation_ID      TEXT,
    Instrument          TEXT,
    Side                TEXT,
    Units               INTEGER,
    Created_At          TEXT
);

ALTER TABLE Fact_Live_Trades ADD COLUMN Broker_Order_ID    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Broker_Trade_ID    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Fill_Price         REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Fill_Time          TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Requested_Price    REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Slippage_Pips      REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Realized_Status    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Filled_Units       INTEGER;
ALTER TABLE Fact_Live_Trades ADD COLUMN Stop_Loss_Price    REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Take_Profit_Price  REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Model_Threshold    REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Model_Set_ID       TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Regime_Label       TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Correlation_Score  REAL;
ALTER TABLE Fact_Live_Trades ADD COLUMN Correlation_Passed INTEGER;
ALTER TABLE Fact_Live_Trades ADD COLUMN Reject_Reason      TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN Updated_At         TEXT;
