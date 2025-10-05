from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import io

# ----------------------------------------------------------------------
# 1. Embedded Telemetry Data in the required Array of Objects format
#    - Includes region, latency_ms, and uptime_percentage.
# ----------------------------------------------------------------------
TELEMETRY_JSON_DATA = """
[
  {
    "region": "apac",
    "service": "recommendations",
    "latency_ms": 130,
    "uptime_pct": 99.149,
    "timestamp": 20250301
  },
  {
    "region": "apac",
    "service": "support",
    "latency_ms": 188.13,
    "uptime_pct": 97.221,
    "timestamp": 20250302
  },
  {
    "region": "apac",
    "service": "catalog",
    "latency_ms": 172.96,
    "uptime_pct": 98.622,
    "timestamp": 20250303
  },
  {
    "region": "apac",
    "service": "checkout",
    "latency_ms": 125.77,
    "uptime_pct": 98.592,
    "timestamp": 20250304
  },
  {
    "region": "apac",
    "service": "recommendations",
    "latency_ms": 209.93,
    "uptime_pct": 97.551,
    "timestamp": 20250305
  },
  {
    "region": "apac",
    "service": "catalog",
    "latency_ms": 194.52,
    "uptime_pct": 99.266,
    "timestamp": 20250306
  },
  {
    "region": "apac",
    "service": "support",
    "latency_ms": 148.26,
    "uptime_pct": 98.467,
    "timestamp": 20250307
  },
  {
    "region": "apac",
    "service": "checkout",
    "latency_ms": 185,
    "uptime_pct": 98.172,
    "timestamp": 20250308
  },
  {
    "region": "apac",
    "service": "catalog",
    "latency_ms": 203.45,
    "uptime_pct": 97.618,
    "timestamp": 20250309
  },
  {
    "region": "apac",
    "service": "checkout",
    "latency_ms": 229.55,
    "uptime_pct": 97.537,
    "timestamp": 20250310
  },
  {
    "region": "apac",
    "service": "support",
    "latency_ms": 169.07,
    "uptime_pct": 98.758,
    "timestamp": 20250311
  },
  {
    "region": "apac",
    "service": "checkout",
    "latency_ms": 115.53,
    "uptime_pct": 97.194,
    "timestamp": 20250312
  },
  {
    "region": "emea",
    "service": "payments",
    "latency_ms": 172.68,
    "uptime_pct": 98.064,
    "timestamp": 20250301
  },
  {
    "region": "emea",
    "service": "checkout",
    "latency_ms": 226.91,
    "uptime_pct": 97.894,
    "timestamp": 20250302
  },
  {
    "region": "emea",
    "service": "catalog",
    "latency_ms": 198.7,
    "uptime_pct": 97.213,
    "timestamp": 20250303
  },
  {
    "region": "emea",
    "service": "payments",
    "latency_ms": 202.56,
    "uptime_pct": 99.168,
    "timestamp": 20250304
  },
  {
    "region": "emea",
    "service": "payments",
    "latency_ms": 218.36,
    "uptime_pct": 99.242,
    "timestamp": 20250305
  },
  {
    "region": "emea",
    "service": "catalog",
    "latency_ms": 219.24,
    "uptime_pct": 98.575,
    "timestamp": 20250306
  },
  {
    "region": "emea",
    "service": "recommendations",
    "latency_ms": 199.26,
    "uptime_pct": 98.756,
    "timestamp": 20250307
  },
  {
    "region": "emea",
    "service": "analytics",
    "latency_ms": 220.33,
    "uptime_pct": 99.45,
    "timestamp": 20250308
  },
  {
    "region": "emea",
    "service": "support",
    "latency_ms": 128.61,
    "uptime_pct": 97.824,
    "timestamp": 20250309
  },
  {
    "region": "emea",
    "service": "analytics",
    "latency_ms": 178.24,
    "uptime_pct": 98.039,
    "timestamp": 20250310
  },
  {
    "region": "emea",
    "service": "recommendations",
    "latency_ms": 102.91,
    "uptime_pct": 99.325,
    "timestamp": 20250311
  },
  {
    "region": "emea",
    "service": "recommendations",
    "latency_ms": 186.67,
    "uptime_pct": 97.51,
    "timestamp": 20250312
  },
  {
    "region": "amer",
    "service": "payments",
    "latency_ms": 132.35,
    "uptime_pct": 99.43,
    "timestamp": 20250301
  },
  {
    "region": "amer",
    "service": "recommendations",
    "latency_ms": 195.13,
    "uptime_pct": 98.038,
    "timestamp": 20250302
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 109.45,
    "uptime_pct": 97.87,
    "timestamp": 20250303
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 141.82,
    "uptime_pct": 97.711,
    "timestamp": 20250304
  },
  {
    "region": "amer",
    "service": "analytics",
    "latency_ms": 114.98,
    "uptime_pct": 97.152,
    "timestamp": 20250305
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 152.68,
    "uptime_pct": 98.531,
    "timestamp": 20250306
  },
  {
    "region": "amer",
    "service": "catalog",
    "latency_ms": 147.42,
    "uptime_pct": 98.284,
    "timestamp": 20250307
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 139.45,
    "uptime_pct": 99.019,
    "timestamp": 20250308
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 136.68,
    "uptime_pct": 97.336,
    "timestamp": 20250309
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 214.92,
    "uptime_pct": 99.432,
    "timestamp": 20250310
  },
  {
    "region": "amer",
    "service": "analytics",
    "latency_ms": 139.15,
    "uptime_pct": 98.092,
    "timestamp": 20250311
  },
  {
    "region": "amer",
    "service": "support",
    "latency_ms": 154.64,
    "uptime_pct": 98.31,
    "timestamp": 20250312
  }
]
"""

# Initialize FastAPI App
app = FastAPI()

# Input model validation
class LatencyQuery(BaseModel):
    regions: list[str]
    threshold_ms: int

# --- CORS Configuration ---
# Enables CORS for POST requests from any origin (*)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST", "OPTIONS"],  # POST is required, OPTIONS for pre-flight
    allow_headers=["*"],
)

# Load data into a pandas DataFrame once at the module level
df = pd.read_json(io.StringIO(TELEMETRY_JSON_DATA))

# ----------------------------------------------------------------------
# 2. POST Endpoint: /analytics
# ----------------------------------------------------------------------
@app.post("/analytics")
def get_analytics(query: LatencyQuery):
    """Calculates per-region latency and uptime metrics."""
    
    # Filter for requested regions
    filtered_df = df[df['region'].isin(query.regions)].copy()
    
    # Define custom aggregation functions that capture the threshold
    def breaches_count(x):
        """Count records strictly above the threshold."""
        return (x > query.threshold_ms).sum()

    def p95_calc(x):
        """Calculate the 95th percentile."""
        # Pandas uses 'linear' interpolation, which is standard for p95.
        return x.quantile(0.95)

    # Group by region and calculate all required metrics
    metrics = filtered_df.groupby('region').agg(
        avg_latency=('latency_ms', 'mean'),
        p95_latency=('latency_ms', p95_calc),
        avg_uptime=('uptime_percentage', 'mean'),
        breaches=('latency_ms', breaches_count)
    )
    
    # Format, round, and return the results as a list of dictionaries
    results = []
    for region, row in metrics.iterrows():
        results.append({
            "region": region,
            "avg_latency": round(row['avg_latency'], 2),
            "p95_latency": round(row['p95_latency'], 2),
            "avg_uptime": round(row['avg_uptime'], 2),
            "breaches": int(row['breaches'])
        })
        
    return results
    
# Root endpoint for Vercel health check
@app.get("/")
def read_root():
    return {"status": "ok"}
