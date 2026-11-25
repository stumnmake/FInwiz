# FinWiz Live Investment Simulation

A browser-based live investment simulation with **one unified HTML interface** and two role-specific portals:

- **Admin Control Centre** for organisers
- **Player Investment Terminal** for teams

Both portals use the same FastAPI backend, SQLite database and WebSocket live channel. The browser interface is in `static/index.html` and is also provided separately as `FinWiz_Live_Investment_Simulation.html`.

## What it does

- Admin and player login with role separation.
- Admin creates teams; each receives ₹10 lakh own capital + ₹50 lakh simulated loan = ₹60 lakh initial capital.
- Full ₹60 lakh is investible in any available instrument.
- Default annual premium: **1% of ₹50 lakh cover = ₹50,000 per team per year**. The first premium is deducted when a team is created; subsequent annual premiums are deducted/accrued when the admin advances the stage. Admin can change the rate and cover.
- Stage-based buy/sell trading, protected by server-side validation.
- 50 NIFTY 50 starter equity entries, bonds, gold, silver, plus custom instruments.
- Admin can draft asset price changes using percentage movements or direct prices.
- Class-wide scenario changes and instrument-specific overrides.
- Preview of proposed prices and team portfolio impact before market publishing.
- One market publish updates all team portfolios using the same prices.
- Live refresh via WebSocket after trades, stage changes, announcements and market updates.
- Portfolio holdings, transaction history, leaderboard, audit log, announcements and maturity settlement.

## Run locally

Requirements: Python 3.10 or newer.

### Windows

1. Extract this ZIP.
2. Double-click `run_windows.bat`.
3. Open `http://localhost:8000`.
4. Sign in as admin using the initial credentials below.

### macOS / Linux

```bash
cd finwiz-live-simulation
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 run.py
```

Open `http://localhost:8000` in your browser. After login, the application automatically shows the admin or player interface according to the account role. The `/admin` and `/player` paths also open the same unified login page.

## Initial admin login

- Username: `admin`
- Password: `FinWiz@2026!`

**Change this password immediately after first login.** Do not leave the starter password on a public deployment.

## Deploy for teams on different networks

For true live multi-device use, deploy the **backend application**, not only the standalone HTML.

### Render deployment

1. Push the contents of this project folder to a private or public GitHub repository.
2. In Render, create a new **Web Service** from that repository.
3. Set build command: `pip install -r requirements.txt`.
4. Set start command: `uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1`.
5. Attach a persistent disk mounted at `/var/data`.
6. Set environment variable `FINWIZ_DB=/var/data/finwiz_simulation.db`.
7. Deploy and open the HTTPS service URL.

The included `render.yaml` describes this setup. Confirm the hosting plan supports persistent disks. A free ephemeral service may lose the SQLite database after restart/redeploy and is not appropriate for a live competition.

When deployed this way, the same URL serves the HTML and backend. Admins and all teams log into that same URL from their devices; live updates are handled by the shared database and WebSocket.

### Deploying only the HTML on a static host

You may deploy `static/index.html` (or the separately supplied HTML file) to a static host, but first deploy this backend separately. Open the HTML, expand **Connection settings**, enter the backend HTTPS URL, and save it. The static page connects to that shared backend. Do not use separate backend instances for the admin and player portals.

## Simulation rules and cautions

- ₹50,000 annual premium is a configurable **competition assumption**, not an actuarially validated premium or real insurance quote.
- At maturity, the fictional game settlement liquidates portfolio value, clears premiums, repays loan principal and applies an illustrative insurer payout capped at the insured cover. This is not a real insurance contract.
- No interest on the loan, brokerage, taxes, dividends or bond coupons are included in this starter version.
- The NIFTY equity symbols and ₹100 prices are editable starter simulation data, not a verified live constituent feed or live exchange price feed. Verify and adjust the investment universe before the event.
- Before competition day, create practice accounts and test simultaneous access, stage transitions, premiums, trades and market publishing.
- Back up the database before and after every stage. Run one application worker with this SQLite implementation.
