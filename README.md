# GRIDWATCH // US Grid Telemetry, Curtailment & Dispatch API

The US power grid does not expose a unified data stream. It is fractured across dozens of Independent System Operators (ISOs) and regulated Balancing Authorities, each publishing operational telemetry at varying intervals, in conflicting formats, and across different temporal horizons.

GridWatch is a serverless ingestion pipeline that harmonizes this chaos. It scrapes, cleans, and standardizes real-time load (MW) and Locational Marginal Pricing (LMP) across 22 US grid regions, exposing the aggregated state via a low-latency REST API.

Designed for programmatic demand-response, the system includes native endpoint triggers (`/curtailment` and `/dispatch`) for automated load shedding or opportunity charging based on grid stress or price floors.

### Architecture & Infrastructure

The pipeline is built on Azure Serverless infrastructure, managed as code via Terraform, and deployed via GitHub Actions.

* **Compute:** Azure Linux Function App (Consumption Tier). Runs on highly specific cron boundaries (`0 */5 * * * *`) to map directly to standard ISO 5-minute Security-Constrained Economic Dispatch (SCED) cycles.
* **Hot State (API):** Azure Table Storage (`GridStatusLatest`). Allows sub-second reads for the frontend and API consumers without executing expensive file system scans.
* **Cold Storage (Data Lake):** Azure Data Lake Storage Gen2 (Hierarchical Namespace). Raw payloads are partitioned by `Region/YYYY/MM/DD` into a `processed-silver` container for historical big-data analytics.
* **CI/CD Build:** The GitHub Actions pipeline forces a `manylinux2014_x86_64` Python wheel compilation to ensure native C-extension compatibility in the Azure Linux environment before deploying the artifact via a 5-year SAS token.

### Engineering for Upstream Chaos

Third-party public data feeds fail. Endpoints deprecate. Files arrive empty. GridWatch survives upstream volatility through explicit arbitration and diagnostic routing:

* **Temporal Arbitration:** Load and pricing data frequently arrive asynchronously. The engine extracts the raw `timestamp` from the upstream payloads, arbitrates the discrepancy, and calculates explicit `lag_sec` metrics to expose staleness before taking action.
* **Diagnostic Telemetry Integration:** Instead of relying on blind `try/except` blocks that swallow errors, upstream HTTP anomalies or missing JSON keys are captured and appended directly to the API's `status` payload (e.g., `OK (10m)`). Observability is pushed to the client.
* **Protocol Agnosticism:** The ingestion engine handles ZIP-compressed XML (CAISO OASIS), raw CSV parsing via `pandas` (NYISO, SPP file-browser), and authenticated REST APIs (EIA, PJM) simultaneously.

### API Endpoints

* `GET /api/status` - The aggregate state. Returns load, utilization %, LMP, and data lag for all tracked regions.
* `GET /api/curtailment` - Demand-response signal. Returns a boolean `curtail` flag if a specified region breaches a user-defined `price_cap` or `stress_cap`.
* `GET /api/dispatch` - Opportunity charging signal. Returns a boolean `dispatch` flag if local LMPs drop below a user-defined `price_floor` (e.g., $0.00 negative pricing).
* `GET /api/history` - Time-series reconstruction. Returns the trailing 100 operational intervals of load and pricing data for a targeted region.

### Deployment

**1. The Managed Service (RapidAPI)**
For immediate access without the friction of provisioning infrastructure or managing upstream API keys (EIA, PJM), the production endpoint is actively managed and available here:
[GridWatch US Telemetry on RapidAPI](https://rapidapi.com/cnorris1316/api/gridwatch-us-telemetry)

**2. Self-Hosted Infrastructure**
To assume total control over the Medallion architecture and data retention:
1. Initialize the IaC: `cd infra && terraform apply`
2. Secure your upstream keys: Inject `EIA_API_KEY` and `PJM_API_KEY` into your environment.
3. Execute the GitHub Actions workflow to compile the Python environment and sync the static frontend to Azure Blob Storage `$web`.
