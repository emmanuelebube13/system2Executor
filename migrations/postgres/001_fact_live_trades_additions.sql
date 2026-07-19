-- EXEC-006 Fact_Live_Trades additions (PostgreSQL / prod). Additive, nullable, idempotent.
CREATE TABLE IF NOT EXISTS Fact_Live_Trades (
    Order_ID            TEXT PRIMARY KEY,
    Correlation_ID      TEXT,
    Instrument          TEXT,
    Side                TEXT,
    Units               INTEGER,
    Created_At          TIMESTAMPTZ
);

ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Broker_Order_ID    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Broker_Trade_ID    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Fill_Price         NUMERIC(18,6);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Fill_Time          TIMESTAMPTZ;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Requested_Price    NUMERIC(18,6);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Slippage_Pips      NUMERIC(9,2);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Realized_Status    TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Filled_Units       INTEGER;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Stop_Loss_Price    NUMERIC(18,6);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Take_Profit_Price  NUMERIC(18,6);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Model_Threshold    NUMERIC(9,4);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Model_Set_ID       TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Regime_Label       TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Correlation_Score  NUMERIC(9,4);
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Correlation_Passed BOOLEAN;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Reject_Reason      TEXT;
ALTER TABLE Fact_Live_Trades ADD COLUMN IF NOT EXISTS Updated_At         TIMESTAMPTZ;
