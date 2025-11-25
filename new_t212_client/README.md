# new-t212-client

Production-ready Trading 212 API client with SQL Server persistence. Features intelligent incremental data collection, optimized rate limiting, and comprehensive endpoint coverage.

## ✅ Current Status: PRODUCTION READY

- **Duration**: ~1.5 seconds for complete collection
- **API Coverage**: All 6 endpoints (account, portfolio, orders, pies, transactions, history)
- **Data Management**: Smart snapshots + incremental history
- **Rate Limiting**: Optimized 10-second spacing for history endpoints
- **Database**: SQL Server with proper schemas and constraints

## 🚀 Quick Start

1. **Prerequisites**:
   - SQL Server Express (localhost\SQLEXPRESS)
   - Python 3.9+
   - Trading 212 API key

2. **Setup**:
   ```powershell
   cd new_t212_client
   pip install -e .
   # Set T212_API_KEY environment variable
   ```

3. **Run Production Collection**:
   ```powershell
   python run_hourly.py
   ```

## 📊 Production Scripts

### `run_hourly.py` - Main Production Script
- **Complete data collection** in ~1.5 seconds
- **Smart incremental logic**: Only new transactions/orders
- **Snapshot replacement**: Current portfolio/orders/pies
- **Rate limit compliant**: 6 req/min for history endpoints

```powershell
# Output example:
Duration: 1.49 seconds
API calls: 7
Portfolio positions: 4
Pie allocations: 2
New transactions: 0
New orders: 0
```

### `migrate_sqlite_cash_history.py` - Data Migration
- Migrated 806 transactions ✅
- Migrated 3,363 cash snapshots ✅
- Preserves original SQLite database ✅

## 🏗️ Architecture

### **Data Collection Strategy**
- **Snapshots** (Replace): Portfolio, orders, pies, account cash
- **Incremental** (Append): Transactions, order history, dividends
- **Rate Limiting**: 10-second spacing between history calls
- **Performance**: ~1.5 seconds total runtime

### **Database Schema** (SQL Server)
```sql
-- Current State (Snapshots)
core.account_cash_snapshot          # Real-time balances
core.portfolio_position_snapshot    # Holdings with P&L
core.pending_order_snapshot         # Open orders  
core.pie_allocation_snapshot        # Investment pie breakdowns

-- Historical Data (Incremental)
core.transaction_history            # All transactions
core.order_history                  # Order executions
core.dividend_history               # Dividend payments

-- Metadata
core.account_profile                # Account info
core.instrument                     # Stock/ETF metadata
```

## 🔧 Key Components

### **Rate Limiter** (`rate_limiter.py`)
- **History endpoints**: 6 requests per minute (10-second spacing)
- **Real-time endpoints**: No artificial delays
- **Smart logic**: No wait on first call, proper pacing after

### **Endpoints** (`endpoints/`)
- `account.py` - Account info and cash balances
- `portfolio.py` - Holdings and pie allocations
- `history.py` - Transactions and order history  
- `metadata.py` - Instruments and exchanges

### **Storage** (`storage/sql_server.py`)
- Snapshot replacement logic (DELETE + INSERT)
- Incremental append logic with duplicate detection
- Proper SQL Server connection management

## 📈 API Coverage

| Endpoint | Purpose | Type | Status |
|----------|---------|------|--------|
| `/equity/account/info` | Account details | Profile | ✅ |
| `/equity/account/cash` | Cash balances | Snapshot | ✅ |
| `/equity/portfolio` | Holdings & P&L | Snapshot | ✅ |
| `/equity/orders` | Pending orders | Snapshot | ✅ |
| `/equity/pies` | Investment pies | Snapshot | ✅ |
| `/equity/pies/{id}` | Pie allocations | Snapshot | ✅ |
| `/history/transactions` | Transactions | Incremental | ✅ |
| `/equity/history/orders` | Order history | Incremental | ✅ |

## 🚀 Production Deployment

### **Automated Scheduling**
```powershell
# Windows Task Scheduler
schtasks /create /tn "T212Collection" /tr "python C:\path\to\run_hourly.py" /sc hourly
```

### **Monitoring**
- Performance logging (duration, API calls, record counts)
- Error handling with detailed logging
- Data validation and integrity checks

---

**Status**: ✅ Production Ready | **Performance**: ~1.5s | **Reliability**: Stable
