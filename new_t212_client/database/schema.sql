/*
  Trading 212 curated analytics schema for SQL Server.
  The script provisions a landing (staging) area for raw payloads and a curated layer ready for BI.
*/

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'Trading212Analytics')
BEGIN
	EXEC('CREATE DATABASE Trading212Analytics');
END;
GO

USE Trading212Analytics;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'staging')
BEGIN
	EXEC('CREATE SCHEMA staging');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'core')
BEGIN
	EXEC('CREATE SCHEMA core');
END;
GO

/* Landing zone for raw API responses, kept for backfill and troubleshooting. */
IF OBJECT_ID(N'staging.raw_api_payload', N'U') IS NULL
BEGIN
	CREATE TABLE staging.raw_api_payload
	(
		raw_payload_id      BIGINT            IDENTITY(1,1) PRIMARY KEY,
		endpoint            NVARCHAR(128)     NOT NULL,
		account_id          BIGINT            NULL,
		captured_at_utc     DATETIME2(3)      NOT NULL DEFAULT SYSUTCDATETIME(),
		correlation_id      NVARCHAR(128)     NULL,
		payload_hash        BINARY(32)        NULL,
		payload_json        NVARCHAR(MAX)     NOT NULL
	);
	CREATE INDEX IX_raw_api_payload_endpoint_time
		ON staging.raw_api_payload(endpoint, captured_at_utc DESC);
END;
GO

/* Core dimension holding the Trading212 account metadata. */
IF OBJECT_ID(N'core.account_profile', N'U') IS NULL
BEGIN
	CREATE TABLE core.account_profile
	(
		account_id       BIGINT        NOT NULL PRIMARY KEY,
		currency_code    CHAR(3)       NOT NULL,
		first_seen_at    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
		last_seen_at     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
	);
END;
GO

/* Account cash snapshots map the /equity/account/cash endpoint into queryable facts. */
IF OBJECT_ID(N'core.account_cash_snapshot', N'U') IS NULL
BEGIN
	CREATE TABLE core.account_cash_snapshot
	(
		snapshot_id        BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id         BIGINT        NOT NULL,
		captured_at_utc    DATETIME2(3)  NOT NULL,
		blocked_amount     DECIMAL(19,4) NULL,
		free_amount        DECIMAL(19,4) NULL,
		invested_amount    DECIMAL(19,4) NULL,
		pie_cash_amount    DECIMAL(19,4) NULL,
		unrealised_ppl     DECIMAL(19,4) NULL,
		realised_result    DECIMAL(19,4) NULL,
		total_equity       DECIMAL(19,4) NULL,
		source_system      NVARCHAR(16)  NOT NULL DEFAULT N'api',
		payload_json       NVARCHAR(MAX) NULL,
		CONSTRAINT FK_account_cash_snapshot_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);
	CREATE INDEX IX_account_cash_snapshot_account_time
		ON core.account_cash_snapshot(account_id, captured_at_utc DESC);
END;
GO

/* Portfolio snapshots represent /equity/portfolio responses at capture time. */
IF OBJECT_ID(N'core.portfolio_position_snapshot', N'U') IS NULL
BEGIN
	CREATE TABLE core.portfolio_position_snapshot
	(
		position_snapshot_id   BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id             BIGINT        NOT NULL,
		captured_at_utc        DATETIME2(3)  NOT NULL,
		ticker                 NVARCHAR(64)  NOT NULL,
		quantity               DECIMAL(19,8) NULL,
		average_price          DECIMAL(19,6) NULL,
		current_price          DECIMAL(19,6) NULL,
		ppl_amount             DECIMAL(19,4) NULL,
		fx_ppl_amount          DECIMAL(19,4) NULL,
		pie_quantity           DECIMAL(19,8) NULL,
		max_buy_quantity       DECIMAL(19,8) NULL,
		max_sell_quantity      DECIMAL(19,8) NULL,
		initial_fill_date      DATETIME2(3)  NULL,
		frontend_origin        NVARCHAR(16)  NULL,
		payload_json           NVARCHAR(MAX) NULL,
		CONSTRAINT FK_position_snapshot_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE UNIQUE INDEX UX_position_snapshot_account_ticker_time
		ON core.portfolio_position_snapshot(account_id, ticker, captured_at_utc);

	CREATE INDEX IX_position_snapshot_time
		ON core.portfolio_position_snapshot(captured_at_utc DESC);
END;
GO

/* Pending order snapshots capture the real-time /equity/orders view. */
IF OBJECT_ID(N'core.pending_order_snapshot', N'U') IS NULL
BEGIN
	CREATE TABLE core.pending_order_snapshot
	(
		pending_snapshot_id  BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id           BIGINT        NOT NULL,
		captured_at_utc      DATETIME2(3)  NOT NULL,
		order_id             BIGINT        NOT NULL,
		ticker               NVARCHAR(64)  NOT NULL,
		order_type           NVARCHAR(16)  NOT NULL,
		order_status         NVARCHAR(32)  NOT NULL,
		strategy             NVARCHAR(16)  NULL,
		quantity             DECIMAL(19,8) NULL,
		value_amount         DECIMAL(19,4) NULL,
		limit_price          DECIMAL(19,6) NULL,
		stop_price           DECIMAL(19,6) NULL,
		extended_hours       BIT           NOT NULL DEFAULT 0,
		filled_quantity      DECIMAL(19,8) NULL,
		filled_value         DECIMAL(19,4) NULL,
		creation_time_utc    DATETIME2(3)  NULL,
		payload_json         NVARCHAR(MAX) NULL,
		CONSTRAINT FK_pending_order_snapshot_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE UNIQUE INDEX UX_pending_order_snapshot_order_capture
		ON core.pending_order_snapshot(order_id, captured_at_utc);

	CREATE INDEX IX_pending_order_snapshot_account_status
		ON core.pending_order_snapshot(account_id, order_status);
END;
GO

/* Historical order fills mapped from /equity/history/orders. */
IF OBJECT_ID(N'core.order_history', N'U') IS NULL
BEGIN
	CREATE TABLE core.order_history
	(
		order_history_id    BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id          BIGINT        NOT NULL,
		order_id            BIGINT        NOT NULL,
		parent_order_id     BIGINT        NULL,
		ticker              NVARCHAR(64)  NOT NULL,
		order_type          NVARCHAR(16)  NOT NULL,
		order_status        NVARCHAR(32)  NOT NULL,
		time_validity       NVARCHAR(20)  NULL,
		executor            NVARCHAR(16)  NULL,
		extended_hours      BIT           NOT NULL DEFAULT 0,
		ordered_quantity    DECIMAL(19,8) NULL,
		ordered_value       DECIMAL(19,4) NULL,
		filled_quantity     DECIMAL(19,8) NULL,
		filled_value        DECIMAL(19,4) NULL,
		fill_price          DECIMAL(19,6) NULL,
		fill_cost           DECIMAL(19,4) NULL,
		fill_result         DECIMAL(19,4) NULL,
		fill_type           NVARCHAR(32)  NULL,
		fill_id             BIGINT        NULL,
		limit_price         DECIMAL(19,6) NULL,
		stop_price          DECIMAL(19,6) NULL,
		placed_at_utc       DATETIME2(3)  NULL,
		executed_at_utc     DATETIME2(3)  NULL,
		modified_at_utc     DATETIME2(3)  NULL,
		payload_json        NVARCHAR(MAX) NULL,
		ingestion_time_utc  DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
		CONSTRAINT FK_order_history_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE UNIQUE INDEX UX_order_history_order_fill
		ON core.order_history(order_id, fill_id)
		WHERE fill_id IS NOT NULL;

	CREATE INDEX IX_order_history_ticker_time
		ON core.order_history(ticker, executed_at_utc DESC);
END;
GO

IF OBJECT_ID(N'core.order_history_tax', N'U') IS NULL
BEGIN
	CREATE TABLE core.order_history_tax
	(
		order_history_tax_id  BIGINT        IDENTITY(1,1) PRIMARY KEY,
		order_history_id      BIGINT        NOT NULL,
		fill_id               NVARCHAR(64)  NULL,
		tax_name              NVARCHAR(64)  NOT NULL,
		tax_quantity          DECIMAL(19,6) NOT NULL,
		time_charged_utc      DATETIME2(3)  NULL,
		payload_json          NVARCHAR(MAX) NULL,
		CONSTRAINT FK_order_history_tax_order
			FOREIGN KEY (order_history_id) REFERENCES core.order_history(order_history_id)
				ON DELETE CASCADE
	);

	CREATE INDEX IX_order_history_tax_order
		ON core.order_history_tax(order_history_id);
END;
GO

/* Dividend history mapped from /history/dividends. */
IF OBJECT_ID(N'core.dividend_history', N'U') IS NULL
BEGIN
	CREATE TABLE core.dividend_history
	(
		dividend_history_id    BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id             BIGINT        NOT NULL,
		reference              NVARCHAR(64)  NOT NULL,
		ticker                 NVARCHAR(64)  NOT NULL,
		dividend_type          NVARCHAR(32)  NULL,
		quantity               DECIMAL(19,8) NULL,
		gross_amount_per_share DECIMAL(19,6) NULL,
		amount_account_ccy     DECIMAL(19,4) NOT NULL,
		amount_eur             DECIMAL(19,4) NULL,
		paid_on_utc            DATETIME2(3)  NOT NULL,
		payload_json           NVARCHAR(MAX) NULL,
		ingestion_time_utc     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
		CONSTRAINT FK_dividend_history_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE UNIQUE INDEX UX_dividend_history_reference
		ON core.dividend_history(reference, ticker);
END;
GO

/* Transaction history from /history/transactions. */
IF OBJECT_ID(N'core.transaction_history', N'U') IS NULL
BEGIN
	CREATE TABLE core.transaction_history
	(
		transaction_history_id  BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id              BIGINT        NOT NULL,
		reference               NVARCHAR(64)  NOT NULL,
		transaction_type        NVARCHAR(32)  NOT NULL,
		amount_account_ccy      DECIMAL(19,4) NOT NULL,
		occurred_at_utc         DATETIME2(3)  NOT NULL,
		payload_json            NVARCHAR(MAX) NULL,
		ingestion_time_utc      DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
		CONSTRAINT FK_transaction_history_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE UNIQUE INDEX UX_transaction_history_reference
		ON core.transaction_history(reference, transaction_type);
END;
GO

/* Instruments metadata from /equity/metadata/instruments. */
IF OBJECT_ID(N'core.instrument', N'U') IS NULL
BEGIN
	CREATE TABLE core.instrument
	(
		ticker               NVARCHAR(64)  NOT NULL PRIMARY KEY,
		isin                 NVARCHAR(32)  NULL,
		name                 NVARCHAR(256) NOT NULL,
		short_name           NVARCHAR(128) NULL,
		currency_code        CHAR(3)       NOT NULL,
		instrument_type      NVARCHAR(32)  NOT NULL,
		working_schedule_id  BIGINT        NULL,
		max_open_quantity    DECIMAL(19,8) NULL,
		added_on_utc         DATETIME2(3)  NULL,
		payload_json         NVARCHAR(MAX) NULL
	);
END;
GO

/* Exchange metadata and trading sessions. */
IF OBJECT_ID(N'core.exchange', N'U') IS NULL
BEGIN
	CREATE TABLE core.exchange
	(
		exchange_id   BIGINT         NOT NULL PRIMARY KEY,
		exchange_name NVARCHAR(128)  NOT NULL,
		payload_json  NVARCHAR(MAX)  NULL
	);
END;
GO

IF OBJECT_ID(N'core.working_schedule', N'U') IS NULL
BEGIN
	CREATE TABLE core.working_schedule
	(
		working_schedule_id BIGINT        NOT NULL PRIMARY KEY,
		exchange_id         BIGINT        NOT NULL,
		payload_json        NVARCHAR(MAX) NULL,
		CONSTRAINT FK_working_schedule_exchange
			FOREIGN KEY (exchange_id) REFERENCES core.exchange(exchange_id)
				ON DELETE CASCADE
	);
END;
GO

IF OBJECT_ID(N'core.working_schedule_event', N'U') IS NULL
BEGIN
	CREATE TABLE core.working_schedule_event
	(
		schedule_event_id    BIGINT        IDENTITY(1,1) PRIMARY KEY,
		working_schedule_id  BIGINT        NOT NULL,
		event_type           NVARCHAR(32)  NOT NULL,
		event_time_utc       DATETIME2(3)  NOT NULL,
		payload_json         NVARCHAR(MAX) NULL,
		CONSTRAINT FK_working_schedule_event_schedule
			FOREIGN KEY (working_schedule_id) REFERENCES core.working_schedule(working_schedule_id)
				ON DELETE CASCADE
	);

	CREATE INDEX IX_working_schedule_event_schedule
		ON core.working_schedule_event(working_schedule_id, event_time_utc);
END;
GO

/* Optional lookup to capture pies or strategy groupings when that endpoint becomes available. */
IF OBJECT_ID(N'core.pie_allocation_snapshot', N'U') IS NULL
BEGIN
	CREATE TABLE core.pie_allocation_snapshot
	(
		pie_snapshot_id    BIGINT        IDENTITY(1,1) PRIMARY KEY,
		account_id         BIGINT        NOT NULL,
		captured_at_utc    DATETIME2(3)  NOT NULL,
		pie_id             NVARCHAR(64)  NOT NULL,
		ticker             NVARCHAR(64)  NOT NULL,
		target_weight_pct  DECIMAL(9,6)  NULL,
		actual_weight_pct  DECIMAL(9,6)  NULL,
		quantity           DECIMAL(19,8) NULL,
		payload_json       NVARCHAR(MAX) NULL,
		CONSTRAINT FK_pie_allocation_snapshot_account
			FOREIGN KEY (account_id) REFERENCES core.account_profile(account_id)
	);

	CREATE INDEX IX_pie_allocation_snapshot_account
		ON core.pie_allocation_snapshot(account_id, captured_at_utc DESC);
END;
GO

/* View to expose latest cash snapshot per account for quick BI consumption. */
IF OBJECT_ID(N'core.v_latest_account_cash', N'V') IS NOT NULL
BEGIN
	DROP VIEW core.v_latest_account_cash;
END;
GO

CREATE VIEW core.v_latest_account_cash
AS
SELECT acs.account_id,
	   acs.blocked_amount,
	   acs.free_amount,
	   acs.invested_amount,
	   acs.pie_cash_amount,
	   acs.unrealised_ppl,
	   acs.realised_result,
	   acs.total_equity,
	   acs.captured_at_utc
FROM core.account_cash_snapshot acs
WHERE acs.captured_at_utc = (
	SELECT MAX(acs_inner.captured_at_utc)
	FROM core.account_cash_snapshot acs_inner
	WHERE acs_inner.account_id = acs.account_id
);
GO

/* View to expose active positions at the latest capture time per ticker. */
IF OBJECT_ID(N'core.v_latest_positions', N'V') IS NOT NULL
BEGIN
	DROP VIEW core.v_latest_positions;
END;
GO

CREATE VIEW core.v_latest_positions
AS
SELECT p.account_id,
	   p.ticker,
	   p.quantity,
	   p.average_price,
	   p.current_price,
	   p.ppl_amount,
	   p.fx_ppl_amount,
	   p.pie_quantity,
	   p.max_buy_quantity,
	   p.max_sell_quantity,
	   p.initial_fill_date,
	   p.frontend_origin,
	   p.captured_at_utc
FROM core.portfolio_position_snapshot p
WHERE p.captured_at_utc = (
	SELECT MAX(p_inner.captured_at_utc)
	FROM core.portfolio_position_snapshot p_inner
	WHERE p_inner.account_id = p.account_id
	  AND p_inner.ticker = p.ticker
);
GO
