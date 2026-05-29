import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(
    page_title="Ecommerce Analytics",
    layout="wide"
)

DATA_LAKE = Path("/data-lake")
DUCKDB_PATH = DATA_LAKE / "warehouse.duckdb"

# ----------------------------
# CONNECTION
# ----------------------------
@st.cache_resource
def get_conn():
    return duckdb.connect(str(DUCKDB_PATH))

conn = get_conn()

# ----------------------------
# HELPERS
# ----------------------------
def run_query(query):
    try:
        return conn.execute(query).df()
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()

def get_date_bounds():
    df = run_query("SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date FROM marts.fact_orders")
    if df.empty or df["min_date"].isna().all():
        today = datetime.today().date()
        return today - timedelta(days=30), today
    return df.iloc[0]["min_date"], df.iloc[0]["max_date"]

def format_pct(x):
    return f"{x:.2%}" if x is not None else "N/A"

# ----------------------------
# SIDEBAR FILTERS
# ----------------------------
st.sidebar.title("Filters")

min_date, max_date = get_date_bounds()

start_date = st.sidebar.date_input("Start Date", min_date)
end_date = st.sidebar.date_input("End Date", max_date)

date_filter = f"""
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
"""

# ----------------------------
# TITLE
# ----------------------------
st.title("🛒 Ecommerce Analytics Dashboard")

# ----------------------------
# TABS
# ----------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Overview", "Funnel", "Customers", "Products"
])

# =========================================================
# 🟢 TAB 1 — OVERVIEW
# =========================================================
with tab1:
    st.subheader("Executive Overview")

    kpi_query = f"""
    SELECT
        SUM(order_total_amount) AS revenue,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT user_id) AS customers
    FROM marts.fact_orders
    {date_filter}
    """
    kpis = run_query(kpi_query)

    if not kpis.empty:
        revenue = kpis["revenue"][0] or 0
        orders = kpis["orders"][0] or 0
        customers = kpis["customers"][0] or 0
        aov = revenue / orders if orders else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Revenue", f"${revenue:,.0f}")
        col2.metric("Orders", f"{orders:,}")
        col3.metric("Customers", f"{customers:,}")
        col4.metric("AOV", f"${aov:,.2f}")

    trend_query = f"""
    SELECT
        strftime(order_ts, '%Y-%m-%d %H') as order_ts,
        SUM(order_total_amount) AS revenue,
        COUNT(DISTINCT order_id) AS orders
    FROM marts.fact_orders
    {date_filter}
    GROUP BY 1
    ORDER BY 1
    """
    trend_df = run_query(trend_query)

    if not trend_df.empty:
        st.subheader("Orders Over Time")
        st.line_chart(trend_df.set_index("order_ts")[["orders"]])

    cum_trend_query =f"""
    with base as (
        SELECT
            strftime(order_ts, '%Y-%m-%d %H') as order_ts,
            SUM(order_total_amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders
        FROM marts.fact_orders
        {date_filter}
        GROUP BY 1
    )

    SELECT
        order_ts,
        revenue,
        SUM(revenue) OVER (ORDER BY order_ts) AS cumulative_revenue,
        orders,
        SUM(orders) OVER (ORDER BY order_ts) AS cumulative_orders
    FROM base
    ORDER BY order_ts
    """
    cum_trend_df = run_query(cum_trend_query)

    if not trend_df.empty:
        st.subheader("Cumulative Revenue & Orders Over Time")
        st.line_chart(cum_trend_df.set_index("order_ts")[["cumulative_revenue", "cumulative_orders"]])

# =========================================================
# 🔵 TAB 2 — FUNNEL
# =========================================================
with tab2:
    st.subheader("Conversion Funnel")

    # NEED TO ADD FUNNEL INFO
    # funnel_query = """
    # SELECT
    #     COUNT(DISTINCT session_id) AS sessions,
    #     COUNT(DISTINCT CASE WHEN viewed_product THEN session_id END) AS product_views,
    #     COUNT(DISTINCT CASE WHEN added_to_cart THEN session_id END) AS carts,
    #     COUNT(DISTINCT CASE WHEN checkout_started THEN session_id END) AS checkouts,
    #     COUNT(DISTINCT CASE WHEN purchased THEN session_id END) AS purchases
    # FROM fact_sessions
    # """
    # funnel = run_query(funnel_query)

    # if not funnel.empty:
    #     f = funnel.iloc[0]

    #     stages = ["Sessions", "Product Views", "Carts", "Checkouts", "Purchases"]
    #     values = [
    #         f["sessions"],
    #         f["product_views"],
    #         f["carts"],
    #         f["checkouts"],
    #         f["purchases"]
    #     ]

    #     funnel_df = pd.DataFrame({
    #         "stage": stages,
    #         "value": values
    #     })

    #     st.bar_chart(funnel_df.set_index("stage"))

    #     if f["sessions"]:
    #         conversion = f["purchases"] / f["sessions"]
    #         st.metric("Overall Conversion Rate", format_pct(conversion))

# =========================================================
# 🟣 TAB 3 — CUSTOMERS
# =========================================================
with tab3:
    st.subheader("Customer Analytics")

    # --- LTV Distribution
    ltv_query = f"""
    SELECT
        customer_id,
        SUM(order_total_amount) AS lifetime_value
    FROM marts.fact_orders
    {date_filter}
    GROUP BY 1
    """
    ltv_df = run_query(ltv_query)

    if not ltv_df.empty:
        st.write("### LTV Distribution")
        st.bar_chart(ltv_df["lifetime_value"])

    # --- Repeat vs New
    repeat_query = f"""
    SELECT
        COUNT(*) FILTER (WHERE order_count = 1) AS one_time,
        COUNT(*) FILTER (WHERE order_count > 1) AS repeat
    FROM (
        SELECT user_id, COUNT(*) AS order_count
        FROM marts.fact_orders
        {date_filter}
        GROUP BY 1
    )
    """
    repeat_df = run_query(repeat_query)

    if not repeat_df.empty:
        r = repeat_df.iloc[0]
        st.write("### Customer Types")
        st.write({
            "One-time": int(r["one_time"]),
            "Repeat": int(r["repeat"])
        })

# =========================================================
# 🟡 TAB 4 — PRODUCTS
# =========================================================
with tab4:
    st.subheader("Product Insights")

    #NEED TO ADD BETTER PRODUCT INFO
    # product_query = f"""
    # SELECT
    #     product_name,
    #     SUM(revenue) AS revenue
    # FROM fact_order_items
    # {date_filter}
    # GROUP BY 1
    # ORDER BY revenue DESC
    # LIMIT 10
    # """
    # prod_df = run_query(product_query)

    # if not prod_df.empty:
    #     st.write("### Top Products")
    #     st.bar_chart(prod_df.set_index("product_name"))

# ----------------------------
# FOOTER
# ----------------------------
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")