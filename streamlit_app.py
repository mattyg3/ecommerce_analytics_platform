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
    page_icon="🛒",
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
    df = run_query("""
        SELECT
            MIN(order_date) AS min_date,
            MAX(order_date) AS max_date
        FROM marts.fact_orders
    """)

    if df.empty or df["min_date"].isna().all():
        today = datetime.today().date()
        return today - timedelta(days=30), today

    min_date = pd.to_datetime(df.iloc[0]["min_date"]).date()
    max_date = pd.to_datetime(df.iloc[0]["max_date"]).date()

    return min_date, max_date


def pct_change(current, previous):
    if previous is None or pd.isna(previous) or previous == 0:
        return None

    return (current - previous) / previous


def format_delta(value):
    if value is None or pd.isna(value):
        return None

    return f"{value:+.1%}"


def get_previous_period(start_date, end_date):
    """
    Creates an immediately preceding period of the same length.

    Example:
        Current: 2026-09-01 → 2026-09-23
        Previous: 2026-08-09 → 2026-08-31
    """

    days = (end_date - start_date).days + 1

    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)

    return previous_start, previous_end


def get_time_granularity(start_date, end_date):
    """
    Automatically chooses chart granularity based on
    the selected date range.
    """

    days = (end_date - start_date).days + 1

    if days <= 7:
        return "hour"

    elif days <= 31:
        return "day"

    elif days <= 180:
        return "week"

    else:
        return "month"


def get_time_expression(granularity):
    if granularity == "hour":
        return "date_trunc('hour', order_ts)"

    elif granularity == "day":
        return "date_trunc('day', order_ts)"

    elif granularity == "week":
        return "date_trunc('week', order_ts)"

    else:
        return "date_trunc('month', order_ts)"


def format_period_label(granularity):
    if granularity == "hour":
        return "%Y-%m-%d %H:%M"

    elif granularity == "day":
        return "%Y-%m-%d"

    elif granularity == "week":
        return "%Y-%m-%d"

    else:
        return "%Y-%m"

def get_period_data_days(start_date, end_date):
    """
    Return the number of distinct calendar days with data
    in the specified period.
    """
    query = f"""
        SELECT COUNT(DISTINCT order_date) AS data_days
        FROM marts.fact_orders
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """

    df = run_query(query)

    if df.empty or pd.isna(df.iloc[0]["data_days"]):
        return 0

    return int(df.iloc[0]["data_days"])

def get_valid_previous_period(start_date, end_date):
    """
    Return the previous period only if it contains the same
    number of days with data as the selected period.

    Example:
        Selected: 3 days
        Previous: 1 day
        -> return None

        Selected: 3 days
        Previous: 3 days
        -> return previous period
    """
    selected_days = (end_date - start_date).days + 1

    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=selected_days - 1)

    previous_data_days = get_period_data_days(
        previous_start,
        previous_end
    )

    if previous_data_days < selected_days:
        return None, None

    return previous_start, previous_end

# ----------------------------
# SIDEBAR FILTERS
# ----------------------------
st.sidebar.title("Filters")

min_date, max_date = get_date_bounds()

start_date = st.sidebar.date_input(
    "Start Date",
    min_date
)

end_date = st.sidebar.date_input(
    "End Date",
    max_date
)

if start_date > end_date:
    st.error("Start Date must be before End Date.")
    st.stop()


date_filter = f"""
WHERE date BETWEEN '{start_date}' AND '{end_date}'
"""


# ----------------------------
# TITLE
# ----------------------------
st.title("🛒 Ecommerce Analytics Dashboard")

if min_date and max_date:
    st.markdown(
        f"**Total Data coverage:** "
        f"{min_date.strftime('%b %d, %Y')} – "
        f"{max_date.strftime('%b %d, %Y')}"
    )

st.markdown(
        f"**Filtered Data coverage:** "
        f"{start_date.strftime('%b %d, %Y')} – "
        f"{end_date.strftime('%b %d, %Y')}"
    )

# ----------------------------
# TABS
# ----------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Overview",
    "Funnel",
    "Customers",
    "Products"
])


# =========================================================
# 🟢 TAB 1 — OVERVIEW
# =========================================================
with tab1:

    st.subheader("Executive Overview")

    revenue_delta = None
    orders_delta = None
    customers_delta = None
    aov_delta = None

    # -----------------------------------------------------
    # PERIOD COMPARISON
    # -----------------------------------------------------
    previous_start, previous_end = get_valid_previous_period(
        start_date,
        end_date
    )

    if previous_start is not None:
        previous_date_filter = f"""
            WHERE date BETWEEN '{previous_start}' AND '{previous_end}'
        """
    else:
        previous_date_filter = None


    # =====================================================
    # CURRENT PERIOD KPIs
    # =====================================================

    # Revenue and orders come from the daily dbt KPI model.
    # Customers remain a DISTINCT user calculation because
    # daily customer counts cannot be summed across days
    # without double-counting repeat purchasers.

    kpi_query = f"""
        SELECT
            COALESCE(SUM(revenue), 0) AS revenue,
            COALESCE(SUM(orders), 0) AS orders
        FROM marts.metrics_daily_kpis
        {date_filter}
    """

    kpis = run_query(kpi_query)


    customer_query = f"""
        SELECT
            COUNT(DISTINCT user_id) AS customers
        FROM marts.fact_orders
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """

    customer_df = run_query(customer_query)


    # -----------------------------------------------------
    # PREVIOUS PERIOD KPIs
    # -----------------------------------------------------
    if previous_date_filter is not None:

        previous_kpi_query = f"""
            SELECT
                COALESCE(SUM(revenue), 0) AS revenue,
                COALESCE(SUM(orders), 0) AS orders
            FROM marts.metrics_daily_kpis
            {previous_date_filter}
        """

        previous_kpis = run_query(
            previous_kpi_query
        )


        previous_customer_query = f"""
            SELECT
                COUNT(DISTINCT user_id) AS customers
            FROM marts.fact_orders
            WHERE order_date BETWEEN '{previous_start}' AND '{previous_end}'
        """

        previous_customer_df = run_query(
            previous_customer_query
        )

    else:
        previous_kpis = pd.DataFrame()
        previous_customer_df = pd.DataFrame()


    # -----------------------------------------------------
    # CALCULATE KPI VALUES
    # -----------------------------------------------------
    if not kpis.empty:

        revenue = float(
            kpis.iloc[0]["revenue"] or 0
        )

        orders = int(
            kpis.iloc[0]["orders"] or 0
        )

        if not customer_df.empty:
            customers = int(
                customer_df.iloc[0]["customers"] or 0
            )
        else:
            customers = 0

        aov = (
            revenue / orders
            if orders
            else 0
        )


        # -------------------------------------------------
        # PREVIOUS PERIOD VALUES
        # -------------------------------------------------
        if not previous_kpis.empty:

            previous_revenue = float(
                previous_kpis.iloc[0]["revenue"] or 0
            )

            previous_orders = int(
                previous_kpis.iloc[0]["orders"] or 0
            )

            if not previous_customer_df.empty:
                previous_customers = int(
                    previous_customer_df.iloc[0]["customers"] or 0
                )
            else:
                previous_customers = 0

            previous_aov = (
                previous_revenue / previous_orders
                if previous_orders
                else 0
            )

        else:

            previous_revenue = 0
            previous_orders = 0
            previous_customers = 0
            previous_aov = 0


        # -------------------------------------------------
        # KPI DELTAS
        # -------------------------------------------------
        revenue_delta = pct_change(
            revenue,
            previous_revenue
        )

        orders_delta = pct_change(
            orders,
            previous_orders
        )

        customers_delta = pct_change(
            customers,
            previous_customers
        )

        aov_delta = pct_change(
            aov,
            previous_aov
        )


        # -------------------------------------------------
        # KPI CARDS
        # -------------------------------------------------
        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Revenue",
            f"${revenue:,.0f}",
            format_delta(revenue_delta)
        )

        col2.metric(
            "Orders",
            f"{orders:,}",
            format_delta(orders_delta)
        )

        col3.metric(
            "Customers",
            f"{customers:,}",
            format_delta(customers_delta)
        )

        col4.metric(
            "AOV",
            f"${aov:,.2f}",
            format_delta(aov_delta)
        )


    st.divider()


    # =====================================================
    # REVENUE PERFORMANCE
    # =====================================================
    st.subheader("Revenue Performance")

    granularity = get_time_granularity(
        start_date,
        end_date
    )

    # metrics_daily_kpis is daily grain, so aggregate its
    # daily revenue/orders according to the selected period.

    if granularity == "hour":
        # The dbt KPI model is daily, so hourly revenue cannot
        # be reconstructed from this model.
        time_expression = None

    elif granularity == "day":
        time_expression = "date"

    elif granularity == "week":
        time_expression = "DATE_TRUNC('week', date)"

    else:
        time_expression = "DATE_TRUNC('month', date)"


    if time_expression is not None:

        trend_query = f"""
            SELECT
                {time_expression} AS period,
                SUM(revenue) AS revenue,
                SUM(orders) AS orders
            FROM marts.metrics_daily_kpis
            {date_filter}
            GROUP BY 1
            ORDER BY 1
        """

        trend_df = run_query(
            trend_query
        )

    else:

        # metrics_daily_kpis does not contain hourly metrics.
        # Fall back to fact_orders only when the dashboard
        # requests hourly granularity.
        trend_query = f"""
            SELECT
                DATE_TRUNC('hour', order_ts) AS period,
                SUM(order_total_amount) AS revenue,
                COUNT(DISTINCT order_id) AS orders
            FROM marts.fact_orders
            WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1
            ORDER BY 1
        """

        trend_df = run_query(
            trend_query
        )


    if not trend_df.empty:

        trend_df["period"] = pd.to_datetime(
            trend_df["period"]
        )

        trend_df = trend_df.set_index(
            "period"
        )

        # Revenue chart
        st.line_chart(
            trend_df[["revenue"]],
            height=350
        )

        granularity_label = {
            "hour": "Hourly",
            "day": "Daily",
            "week": "Weekly",
            "month": "Monthly"
        }[granularity]

        st.caption(
            f"Revenue trend — {granularity_label} aggregation"
        )

    else:
        st.info(
            "No revenue data available for this period."
        )


    st.divider()


    # =====================================================
    # BUSINESS PERFORMANCE
    # =====================================================
    st.subheader("Business Performance")

    left_col, right_col = st.columns([1.4, 1])


    # -----------------------------------------------------
    # REVENUE BY DAY OF WEEK
    # -----------------------------------------------------
    with left_col:

        st.markdown("#### Revenue by Day")

        # metrics_daily_kpis already contains daily revenue
        # and orders, so no need to query fact_orders here.

        dow_query = f"""
            SELECT
                DAYNAME(date) AS day_name,
                DAYOFWEEK(date) AS day_number,
                SUM(revenue) AS revenue,
                SUM(orders) AS orders
            FROM marts.metrics_daily_kpis
            {date_filter}
            GROUP BY 1, 2
            ORDER BY day_number
        """

        dow_df = run_query(
            dow_query
        )


        if not dow_df.empty:

            # Ensure all days appear in normal order
            day_order = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday"
            ]

            dow_df["day_name"] = pd.Categorical(
                dow_df["day_name"],
                categories=day_order,
                ordered=True
            )

            dow_df = dow_df.sort_values(
                "day_name"
            )

            chart_df = dow_df[
                ["day_name", "revenue"]
            ].set_index(
                "day_name"
            )

            st.bar_chart(
                chart_df,
                height=300
            )

        else:
            st.info(
                "No daily revenue data available."
            )


    # -----------------------------------------------------
    # ORDER METRICS
    # -----------------------------------------------------
    with right_col:

        st.markdown("#### Order Metrics")

        order_metrics_query = f"""
            SELECT
                COALESCE(SUM(orders), 0) AS orders,
                COUNT(*) AS active_days,
                CASE
                    WHEN SUM(orders) > 0
                    THEN SUM(revenue) / SUM(orders)
                    ELSE 0
                END AS avg_order_value
            FROM marts.metrics_daily_kpis
            {date_filter}
              AND orders > 0
        """

        order_metrics_df = run_query(
            order_metrics_query
        )


        # Median AOV is not available in metrics_daily_kpis,
        # so calculate it from fact_orders.

        median_query = f"""
            SELECT
                MEDIAN(order_total_amount) AS median_order_value
            FROM marts.fact_orders
            WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        """

        median_df = run_query(
            median_query
        )


        if not order_metrics_df.empty:

            om = order_metrics_df.iloc[0]

            active_days = int(
                om["active_days"] or 0
            )

            avg_orders_per_day = (
                orders / active_days
                if active_days
                else 0
            )


            if not median_df.empty:
                median_order_value = float(
                    median_df.iloc[0]["median_order_value"] or 0
                )
            else:
                median_order_value = 0


            # Peak revenue day
            if not dow_df.empty:

                peak_day = dow_df.loc[
                    dow_df["revenue"].idxmax(),
                    "day_name"
                ]

            else:
                peak_day = "N/A"


            metric_col1, metric_col2 = st.columns(2)

            metric_col1.metric(
                "Avg Orders / Day",
                f"{avg_orders_per_day:,.0f}"
            )

            metric_col2.metric(
                "Median AOV",
                f"${median_order_value:,.2f}"
            )

            st.metric(
                "Peak Revenue Day",
                str(peak_day)
            )

            st.metric(
                "Avg Order Value",
                f"${float(om['avg_order_value'] or 0):,.2f}"
            )


    st.divider()


    # =====================================================
    # CUSTOMER REVENUE MIX
    # =====================================================
    st.subheader("Customer Revenue Mix")

    customer_mix_query = f"""
        WITH customer_orders AS (
            SELECT
                user_id,
                COUNT(DISTINCT order_id) AS order_count,
                SUM(order_total_amount) AS revenue
            FROM marts.fact_orders
            WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY user_id
        )

        SELECT
            CASE
                WHEN order_count = 1 THEN 'New / One-Time'
                ELSE 'Repeat'
            END AS customer_type,
            SUM(revenue) AS revenue,
            COUNT(*) AS customers
        FROM customer_orders
        GROUP BY 1
    """

    customer_mix_df = run_query(
        customer_mix_query
    )


    if not customer_mix_df.empty:

        total_mix_revenue = customer_mix_df[
            "revenue"
        ].sum()

        if total_mix_revenue > 0:

            customer_mix_df["percentage"] = (
                customer_mix_df["revenue"]
                / total_mix_revenue
            )

        else:

            customer_mix_df["percentage"] = 0


        mix_col1, mix_col2 = st.columns([1.5, 1])

        with mix_col1:

            chart_df = customer_mix_df[
                ["customer_type", "revenue"]
            ].set_index(
                "customer_type"
            )

            st.bar_chart(
                chart_df,
                height=250
            )

        with mix_col2:

            for _, row in customer_mix_df.iterrows():

                st.markdown(
                    f"""
                    **{row['customer_type']}**

                    ### ${row['revenue']:,.0f}

                    {row['percentage']:.1%} of revenue
                    """
                )


    st.divider()


    # =====================================================
    # KEY INSIGHTS
    # =====================================================
    st.subheader("Key Insights")

    insights = []


    # -----------------------------------------------------
    # REVENUE INSIGHT
    # -----------------------------------------------------
    if revenue_delta is not None:

        direction = (
            "increased"
            if revenue_delta >= 0
            else "decreased"
        )

        insights.append(
            f"Revenue {direction} "
            f"{abs(revenue_delta):.1%} "
            f"vs. the previous period."
        )


    # -----------------------------------------------------
    # AOV INSIGHT
    # -----------------------------------------------------
    if aov_delta is not None:

        direction = (
            "increased"
            if aov_delta >= 0
            else "decreased"
        )

        insights.append(
            f"AOV {direction} "
            f"{abs(aov_delta):.1%} "
            f"vs. the previous period."
        )


    # -----------------------------------------------------
    # PEAK DAY INSIGHT
    # -----------------------------------------------------
    if not dow_df.empty:

        peak_row = dow_df.loc[
            dow_df["revenue"].idxmax()
        ]

        peak_day_name = str(
            peak_row["day_name"]
        )

        peak_day_revenue = float(
            peak_row["revenue"]
        )

        insights.append(
            f"{peak_day_name} generated the "
            f"highest revenue at "
            f"${peak_day_revenue:,.0f}."
        )


    # -----------------------------------------------------
    # CUSTOMER MIX INSIGHT
    # -----------------------------------------------------
    if not customer_mix_df.empty:

        repeat_rows = customer_mix_df[
            customer_mix_df["customer_type"] == "Repeat"
        ]

        if not repeat_rows.empty:

            repeat_pct = float(
                repeat_rows.iloc[0]["percentage"]
            )

            insights.append(
                f"Repeat customers generated "
                f"{repeat_pct:.1%} of total revenue."
            )


    # -----------------------------------------------------
    # DISPLAY INSIGHTS
    # -----------------------------------------------------
    if insights:

        for insight in insights:
            st.markdown(
                f"• {insight}"
            )

    else:

        st.info(
            "Not enough data to generate insights."
        )


# =========================================================
# 🔵 TAB 2 — FUNNEL
# =========================================================
with tab2:

    st.subheader("Conversion Funnel")

    # st.caption(
    #     f"Session conversion from site visit to purchase "
    #     f"for {start_date.strftime('%b %d, %Y')} – "
    #     f"{end_date.strftime('%b %d, %Y')}"
    # )

    # -----------------------------------------------------
    # FUNNEL QUERY
    # -----------------------------------------------------
    funnel_query = f"""
        SELECT
            SUM(sessions) AS sessions,
            SUM(sessions_with_product_view) AS product_views,
            SUM(sessions_with_add_to_cart) AS add_to_carts,
            SUM(sessions_with_checkout) AS checkouts,
            SUM(sessions_with_order) AS purchases

        FROM marts.metrics_daily_funnel

        {date_filter}
    """

    funnel_df = run_query(funnel_query)

    if funnel_df.empty:
        st.info("No funnel data available for this period.")

    else:

        funnel = funnel_df.iloc[0]

        sessions = int(funnel["sessions"] or 0)
        product_views = int(funnel["product_views"] or 0)
        add_to_carts = int(funnel["add_to_carts"] or 0)
        checkouts = int(funnel["checkouts"] or 0)
        purchases = int(funnel["purchases"] or 0)

        # -------------------------------------------------
        # FUNNEL DATA
        # -------------------------------------------------
        stages = [
            ("Sessions", sessions),
            ("Product Views", product_views),
            ("Add to Cart", add_to_carts),
            ("Checkout", checkouts),
            ("Purchase", purchases),
        ]

        # -------------------------------------------------
        # OVERALL CONVERSION
        # -------------------------------------------------
        overall_conversion = (
            purchases / sessions
            if sessions
            else 0
        )

        st.metric(
            "Overall Conversion Rate",
            f"{overall_conversion:.2%}"
        )

        st.divider()

        # -------------------------------------------------
        # FUNNEL VISUAL
        # -------------------------------------------------
        max_value = max(
            value for _, value in stages
        )

        for i, (stage, value) in enumerate(stages):

            if i == 0:
                conversion = 1.0
                dropoff = 0.0
            else:
                previous_value = stages[i - 1][1]

                conversion = (
                    value / previous_value
                    if previous_value
                    else 0
                )

                dropoff = 1 - conversion

            width_pct = (
                value / max_value * 100
                if max_value
                else 0
            )

            # Keep very small stages visible
            width_pct = max(width_pct, 8)

            col1, col2, col3 = st.columns(
                [2, 6, 2]
            )

            with col1:
                st.markdown(
                    f"**{stage}**"
                )

            with col2:

                st.markdown(
                    f"""
                    <div style="
                        width: 100%;
                        background-color: #E9ECEF;
                        border-radius: 6px;
                        height: 42px;
                        overflow: hidden;
                    ">
                        <div style="
                            width: {width_pct:.1f}%;
                            background-color: #4C78A8;
                            height: 42px;
                            border-radius: 6px;
                            display: flex;
                            align-items: center;
                            padding-left: 12px;
                            color: white;
                            font-weight: 600;
                        ">
                            {value:,}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            with col3:

                if i == 0:

                    st.caption(
                        "100% of sessions"
                    )

                else:

                    st.caption(
                        f"{conversion:.1%} from previous"
                    )


        st.divider()

        # -------------------------------------------------
        # FUNNEL DROP-OFF METRICS
        # -------------------------------------------------
        st.subheader("Stage Conversion")

        conversion_cols = st.columns(4)

        conversion_pairs = [
            ("Sessions → Product View", sessions, product_views),
            ("Product View → Cart", product_views, add_to_carts),
            ("Cart → Checkout", add_to_carts, checkouts),
            ("Checkout → Purchase", checkouts, purchases),
        ]

        for col, (label, previous, current) in zip(
            conversion_cols,
            conversion_pairs
        ):

            rate = (
                current / previous
                if previous
                else 0
            )

            col.metric(
                label,
                f"{rate:.1%}"
            )

# =========================================================
# 🟣 TAB 3 — CUSTOMERS
# =========================================================
with tab3:

    st.subheader("Customer Analytics")

    st.caption("Customer behavior and lifetime value based on the complete customer history.")

    # st.caption(
    #     f"Customer behavior for "
    #     f"{start_date.strftime('%b %d, %Y')} – "
    #     f"{end_date.strftime('%b %d, %Y')}"
    # )


    # =====================================================
    # CUSTOMER KPI SUMMARY
    # =====================================================

    customer_kpi_query = f"""
        SELECT
            COUNT(*) AS customers,
            SUM(lifetime_revenue) AS revenue,
            SUM(total_orders) AS orders,
            AVG(lifetime_revenue) AS avg_customer_value,
            MEDIAN(lifetime_revenue) AS median_customer_value,
            COUNT(*) FILTER (
                WHERE is_repeat_buyer
            ) AS repeat_customers
        FROM marts.metrics_user_lifecycle
    """

    customer_kpi_df = run_query(customer_kpi_query)

    if not customer_kpi_df.empty:

        row = customer_kpi_df.iloc[0]

        total_customers = int(row["customers"] or 0)
        customer_revenue = float(row["revenue"] or 0)
        customer_orders = int(row["orders"] or 0)
        avg_customer_value = float(row["avg_customer_value"] or 0)
        median_customer_value = float(row["median_customer_value"] or 0)
        repeat_customers = int(row["repeat_customers"] or 0)

        repeat_rate = (
            repeat_customers / total_customers
            if total_customers > 0
            else 0
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Customers",
            f"{total_customers:,}"
        )

        col2.metric(
            "Repeat Rate",
            f"{repeat_rate:.1%}"
        )

        col3.metric(
            "Revenue / Customer",
            f"${avg_customer_value:,.2f}"
        )

        col4.metric(
            "Median Customer Value",
            f"${median_customer_value:,.2f}"
        )


    st.divider()


    # =====================================================
    # CUSTOMER VALUE DISTRIBUTION
    # =====================================================

    st.write("### Customer Value Distribution")

    ltv_query = f"""
        SELECT
            user_id,
            lifetime_revenue AS customer_value
        FROM marts.metrics_user_lifecycle
        WHERE total_orders > 0
        ORDER BY customer_value DESC
    """

    ltv_df = run_query(ltv_query)

    if not ltv_df.empty:

        ltv_col1, ltv_col2 = st.columns([2, 1])

        with ltv_col1:

            chart_df = ltv_df[["customer_value"]].copy()

            chart_df["customer_bucket"] = pd.cut(
                chart_df["customer_value"],
                bins=[
                    -float("inf"),
                    50,
                    100,
                    250,
                    500,
                    1000,
                    float("inf")
                ],
                labels=[
                    "< $50",
                    "$50–$100",
                    "$100–$250",
                    "$250–$500",
                    "$500–$1K",
                    "$1K+"
                ]
            )

            distribution_df = (
                chart_df
                .groupby(
                    "customer_bucket",
                    observed=False
                )
                .size()
                .reset_index(name="customers")
            )

            distribution_df = distribution_df.set_index(
                "customer_bucket"
            )

            st.bar_chart(
                distribution_df["customers"],
                height=300
            )

        with ltv_col2:

            st.metric(
                "Highest Customer Value",
                f"${ltv_df['customer_value'].max():,.2f}"
            )

            st.metric(
                "Average Customer Value",
                f"${ltv_df['customer_value'].mean():,.2f}"
            )

            st.metric(
                "Median Customer Value",
                f"${ltv_df['customer_value'].median():,.2f}"
            )


    # =====================================================
    # NEW VS REPEAT CUSTOMERS
    # =====================================================

    st.write("### New vs. Repeat Customers")

    repeat_query = f"""
        SELECT
            CASE
                WHEN is_repeat_buyer
                    THEN 'Repeat'
                ELSE 'One-Time'
            END AS customer_type,

            COUNT(*) AS customers,
            SUM(lifetime_revenue) AS revenue

        FROM marts.metrics_user_lifecycle

        WHERE total_orders > 0

        GROUP BY 1

        ORDER BY
            CASE
                WHEN customer_type = 'One-Time' THEN 1
                ELSE 2
            END
    """

    repeat_df = run_query(repeat_query)

    if not repeat_df.empty:

        repeat_col1, repeat_col2 = st.columns(2)

        with repeat_col1:

            st.write("#### Customer Count")

            customer_count_chart = (
                repeat_df[
                    ["customer_type", "customers"]
                ]
                .set_index("customer_type")
            )

            st.bar_chart(
                customer_count_chart,
                height=250
            )

        with repeat_col2:

            st.write("#### Revenue Contribution")

            revenue_chart = (
                repeat_df[
                    ["customer_type", "revenue"]
                ]
                .set_index("customer_type")
            )

            st.bar_chart(
                revenue_chart,
                height=250
            )


    # =====================================================
    # PURCHASE FREQUENCY
    # =====================================================

    st.write("### Purchase Frequency")

    frequency_query = f"""
        SELECT
            CASE
                WHEN total_orders >= 4 THEN '4+ Orders'
                ELSE CAST(total_orders AS VARCHAR)
                    || ' Order'
                    || CASE
                            WHEN total_orders = 1 THEN ''
                            ELSE 's'
                        END
            END AS purchase_frequency,

            CASE
                WHEN total_orders >= 4 THEN 4
                ELSE total_orders
            END AS sort_order,

            COUNT(*) AS customers

        FROM marts.metrics_user_lifecycle

        WHERE total_orders > 0

        GROUP BY 1, 2

        ORDER BY sort_order
    """

    frequency_df = run_query(frequency_query)

    if not frequency_df.empty:

        frequency_chart = (
            frequency_df[
                ["purchase_frequency", "customers"]
            ]
            .set_index("purchase_frequency")
        )

        st.bar_chart(
            frequency_chart,
            height=300
        )


    # =====================================================
    # CUSTOMER REVENUE CONCENTRATION
    # =====================================================

    st.write("### Customer Revenue Concentration")

    concentration_query = f"""
        WITH ranked_customers AS (

            SELECT
                user_id,
                lifetime_revenue AS revenue,

                SUM(lifetime_revenue) OVER (
                    ORDER BY lifetime_revenue DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
                ) AS cumulative_revenue,

                SUM(lifetime_revenue) OVER () AS total_revenue

            FROM marts.metrics_user_lifecycle

            WHERE total_orders > 0
        )

        SELECT
            user_id,
            revenue,
            cumulative_revenue
                / NULLIF(total_revenue, 0)
                AS cumulative_revenue_pct

        FROM ranked_customers

        ORDER BY revenue DESC
    """

    concentration_df = run_query(
        concentration_query
    )

    if not concentration_df.empty:

        total_revenue = concentration_df["revenue"].sum()

        top_50_count = max(
            1,
            int(len(concentration_df) * 0.50)
        )

        top_50_revenue = (
            concentration_df
            .head(top_50_count)["revenue"]
            .sum()
        )

        top_50_pct = (
            top_50_revenue / total_revenue
            if total_revenue > 0
            else 0
        )

        top_10_count = max(
            1,
            int(len(concentration_df) * 0.10)
        )

        top_10_revenue = (
            concentration_df
            .head(top_10_count)["revenue"]
            .sum()
        )

        top_10_pct = (
            top_10_revenue / total_revenue
            if total_revenue > 0
            else 0
        )

        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Top 10% of Customers",
                f"{top_10_pct:.1%} of Revenue"
            )

        with col2:

            st.metric(
                "Top 50% of Customers",
                f"{top_50_pct:.1%} of Revenue"
            )

        concentration_chart = concentration_df[
            ["cumulative_revenue_pct"]
        ].copy()

        concentration_chart.index = range(
            1,
            len(concentration_chart) + 1
        )

        concentration_chart.columns = [
            "Cumulative Revenue Share"
        ]

        st.line_chart(
            concentration_chart,
            height=300
        )


    # =====================================================
    # TOP CUSTOMERS
    # =====================================================

    st.write("### Top Customers")

    top_customers_query = f"""
        SELECT
            user_id,
            total_orders AS orders,
            lifetime_revenue AS revenue,
            avg_order_value,
            cast(first_order_date as date) as first_order_date,
            cast(last_order_date as date) as last_order_date
        FROM marts.metrics_user_lifecycle
        WHERE total_orders > 0
        ORDER BY lifetime_revenue DESC
        LIMIT 10
    """

    top_customers_df = run_query(
        top_customers_query
    )

    if not top_customers_df.empty:

        display_df = top_customers_df.copy()

        display_df["revenue"] = (
            display_df["revenue"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df["avg_order_value"] = (
            display_df["avg_order_value"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df.columns = [
            "Customer",
            "Orders",
            "Revenue",
            "Avg Order Value",
            "First Order",
            "Last Order"
        ]

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )


     # # =====================================================
    # # COHORT RETENTION ANALYSIS
    # # =====================================================

    # st.write("### Cohort Retention")

    # st.caption(
    #     "Customers are grouped by the month of their first-ever purchase. "
    #     "Retention shows the percentage of each cohort that made a purchase "
    #     "in subsequent months."
    # )

    # cohort_query = f"""
    #     WITH customer_first_purchase AS (

    #         -- Determine each customer's TRUE first-ever purchase.
    #         -- Do NOT apply the dashboard date filter here.
    #         SELECT
    #             user_id,
    #             MIN(order_date) AS first_purchase_date
    #         FROM marts.fact_orders
    #         GROUP BY user_id
    #     ),

    #     customer_cohorts AS (

    #         SELECT
    #             user_id,
    #             DATE_TRUNC(
    #                 'month',
    #                 first_purchase_date
    #             ) AS cohort_month
    #         FROM customer_first_purchase
    #     ),

    #     customer_activity AS (

    #         SELECT DISTINCT
    #             user_id,
    #             DATE_TRUNC(
    #                 'month',
    #                 order_date
    #             ) AS activity_month
    #         FROM marts.fact_orders
    #     ),

    #     cohort_activity AS (

    #         SELECT
    #             c.cohort_month,
    #             a.activity_month,

    #             COUNT(DISTINCT c.user_id) AS retained_customers

    #         FROM customer_cohorts c

    #         INNER JOIN customer_activity a
    #             ON c.user_id = a.user_id

    #         WHERE a.activity_month >= c.cohort_month

    #         GROUP BY
    #             c.cohort_month,
    #             a.activity_month
    #     ),

    #     cohort_sizes AS (

    #         SELECT
    #             cohort_month,
    #             COUNT(*) AS cohort_size
    #         FROM customer_cohorts
    #         GROUP BY cohort_month
    #     )

    #     SELECT
    #         ca.cohort_month,
    #         ca.activity_month,
    #         DATE_DIFF(
    #             'month',
    #             ca.cohort_month,
    #             ca.activity_month
    #         ) AS months_since_first_purchase,
    #         cs.cohort_size,
    #         ca.retained_customers,
    #         ca.retained_customers::DOUBLE
    #             / NULLIF(cs.cohort_size, 0)
    #             AS retention_rate

    #     FROM cohort_activity ca

    #     INNER JOIN cohort_sizes cs
    #         ON ca.cohort_month = cs.cohort_month

    #     ORDER BY
    #         ca.cohort_month,
    #         ca.activity_month
    # """

    # cohort_df = run_query(cohort_query)

    # if not cohort_df.empty:

    #     month_1_df = cohort_df[
    #         cohort_df["months_since_first_purchase"] == 1
    #     ]

    #     month_2_df = cohort_df[
    #         cohort_df["months_since_first_purchase"] == 2
    #     ]

    #     month_3_df = cohort_df[
    #         cohort_df["months_since_first_purchase"] == 3
    #     ]

    #     col1, col2, col3 = st.columns(3)

    #     if not month_1_df.empty:
    #         month_1_retention = (
    #             month_1_df["retained_customers"].sum()
    #             / month_1_df["cohort_size"].sum()
    #         )

    #         col1.metric(
    #             "Month 1 Retention",
    #             f"{month_1_retention:.1%}"
    #         )

    #     if not month_2_df.empty:
    #         month_2_retention = (
    #             month_2_df["retained_customers"].sum()
    #             / month_2_df["cohort_size"].sum()
    #         )

    #         col2.metric(
    #             "Month 2 Retention",
    #             f"{month_2_retention:.1%}"
    #         )

    #     if not month_3_df.empty:
    #         month_3_retention = (
    #             month_3_df["retained_customers"].sum()
    #             / month_3_df["cohort_size"].sum()
    #         )

    #         col3.metric(
    #             "Month 3 Retention",
    #             f"{month_3_retention:.1%}"
    #         )

    #     cohort_pivot = cohort_df.pivot(
    #         index="cohort_month",
    #         columns="months_since_first_purchase",
    #         values="retention_rate"
    #     )

    #     cohort_sizes_display = (
    #         cohort_df[
    #             ["cohort_month", "cohort_size"]
    #         ]
    #         .drop_duplicates()
    #         .set_index("cohort_month")
    #     )

    #     cohort_pivot.index = pd.to_datetime(
    #         cohort_pivot.index
    #     ).strftime("%b %Y")

    #     cohort_sizes_display.index = pd.to_datetime(
    #         cohort_sizes_display.index
    #     ).strftime("%b %Y")

    #     cohort_pivot.columns = [
    #         f"Month {int(col)}"
    #         for col in cohort_pivot.columns
    #     ]

    #     cohort_pivot.insert(
    #         0,
    #         "Cohort Size",
    #         cohort_sizes_display["cohort_size"]
    #     )

    #     # Month 0 should always represent 100% of the cohort.
    #     if "Month 0" in cohort_pivot.columns:
    #         cohort_pivot["Month 0"] = 1.0

    #     # st.dataframe(
    #     #     cohort_pivot.style.format(
    #     #         {
    #     #             "Cohort Size": "{:,.0f}",
    #     #             **{
    #     #                 col: "{:.1%}"
    #     #                 for col in cohort_pivot.columns
    #     #                 if col != "Cohort Size"
    #     #             }
    #     #         },
    #     #         na_rep="—"
    #     #     ),
    #     #     use_container_width=True
    #     # )
    #     styled_cohort = (
    #         cohort_pivot.style
    #         .format(
    #             {
    #                 "Cohort Size": "{:,.0f}",
    #                 **{
    #                     col: "{:.1%}"
    #                     for col in cohort_pivot.columns
    #                     if col != "Cohort Size"
    #                 }
    #             },
    #             na_rep="—"
    #         )
    #         .background_gradient(
    #             subset=[
    #                 col
    #                 for col in cohort_pivot.columns
    #                 if col != "Cohort Size"
    #             ],
    #             axis=None
    #         )
    #     )

    #     st.dataframe(
    #         styled_cohort,
    #         use_container_width=True
    #     )

    # =====================================================
    # BEHAVIORAL ACQUISITION & CONVERSION
    # =====================================================

    st.write("### Behavioral Acquisition & Conversion")

    st.caption(
        "Lifetime customer acquisition and conversion behavior "
        "based on each user's complete history."
    )

    lifecycle_query = """
        SELECT
            user_id,
            first_seen_date,
            first_order_date,
            days_to_first_purchase,
            total_sessions,
            total_orders,
            lifetime_revenue,
            avg_order_value,
            is_repeat_buyer
        FROM marts.metrics_user_lifecycle
    """

    lifecycle_df = run_query(
        lifecycle_query
    )


    if not lifecycle_df.empty:

        # -------------------------------------------------
        # LIFECYCLE METRICS
        # -------------------------------------------------

        total_users = len(lifecycle_df)

        converted_df = lifecycle_df[
            lifecycle_df["first_order_date"].notna()
        ]

        converted_users = len(converted_df)

        conversion_rate = (
            converted_users / total_users
            if total_users > 0
            else 0
        )

        avg_days_to_purchase = (
            converted_df["days_to_first_purchase"].mean()
            if not converted_df.empty
            else 0
        )

        median_days_to_purchase = (
            converted_df["days_to_first_purchase"].median()
            if not converted_df.empty
            else 0
        )

        avg_sessions = (
            lifecycle_df["total_sessions"].mean()
            if total_users > 0
            else 0
        )

        avg_sessions_to_conversion = (
            converted_df["total_sessions"].mean()
            if not converted_df.empty
            else 0
        )

        repeat_buyers = lifecycle_df[
            lifecycle_df["is_repeat_buyer"] == True
        ]

        repeat_rate = (
            len(repeat_buyers) / converted_users
            if converted_users > 0
            else 0
        )


        # -------------------------------------------------
        # KPI CARDS
        # -------------------------------------------------

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Lifetime Conversion Rate",
            f"{conversion_rate:.1%}"
        )

        col2.metric(
            "Avg Days to First Purchase",
            f"{avg_days_to_purchase:.1f}"
        )

        col3.metric(
            "Median Days to First Purchase",
            f"{median_days_to_purchase:.1f}"
        )

        col4.metric(
            "Repeat Buyer Rate",
            f"{repeat_rate:.1%}"
        )


        # -------------------------------------------------
        # CONVERSION FUNNEL
        # -------------------------------------------------

        st.write("#### Customer Acquisition Funnel")

        funnel_df = pd.DataFrame({
            "Stage": [
                "Users Seen",
                "First Purchase",
                "Repeat Purchase"
            ],
            "Customers": [
                total_users,
                converted_users,
                len(repeat_buyers)
            ]
        })

        funnel_chart = funnel_df.set_index(
            "Stage"
        )

        st.bar_chart(
            funnel_chart,
            height=300
        )


        # -------------------------------------------------
        # TIME TO FIRST PURCHASE
        # -------------------------------------------------

        if not converted_df.empty:

            st.write("#### Time to First Purchase")

            conversion_days = (
                converted_df[
                    "days_to_first_purchase"
                ]
                .dropna()
            )

            # Prevent negative or invalid values from
            # appearing in the distribution.
            conversion_days = conversion_days[
                conversion_days >= 0
            ]

            if not conversion_days.empty:

                conversion_distribution = pd.cut(
                    conversion_days,
                    bins=[
                        -1,
                        0,
                        1,
                        3,
                        7,
                        14,
                        30,
                        60,
                        90,
                        float("inf")
                    ],
                    labels=[
                        "Same Day",
                        "1 Day",
                        "2–3 Days",
                        "4–7 Days",
                        "8–14 Days",
                        "15–30 Days",
                        "31–60 Days",
                        "61–90 Days",
                        "90+ Days"
                    ]
                )

                time_to_purchase_df = (
                    conversion_distribution
                    .value_counts(
                        sort=False
                    )
                    .rename("customers")
                    .to_frame()
                )

                st.bar_chart(
                    time_to_purchase_df,
                    height=300
                )


        # -------------------------------------------------
        # SESSIONS & CONVERSION
        # -------------------------------------------------

        st.write("#### Engagement Before Conversion")

        engagement_col1, engagement_col2 = st.columns(2)

        with engagement_col1:

            st.metric(
                "Avg Lifetime Sessions",
                f"{avg_sessions:,.1f}"
            )

        with engagement_col2:

            st.metric(
                "Avg Lifetime Sessions — Converted Users",
                f"{avg_sessions_to_conversion:,.1f}"
            )


        # -------------------------------------------------
        # CUSTOMER LIFECYCLE SUMMARY
        # -------------------------------------------------

        lifecycle_summary = pd.DataFrame({
            "Customer Segment": [
                "Users Seen",
                "Purchased Once or More",
                "Repeat Buyers",
                "Never Purchased"
            ],
            "Customers": [
                total_users,
                converted_users,
                len(repeat_buyers),
                total_users - converted_users
            ]
        })

        lifecycle_summary["Percentage"] = (
            lifecycle_summary["Customers"]
            / total_users
            if total_users > 0
            else 0
        )

        st.write("#### Lifecycle Summary")

        display_lifecycle = lifecycle_summary.copy()

        display_lifecycle["Customers"] = (
            display_lifecycle["Customers"]
            .map(lambda x: f"{x:,}")
        )

        display_lifecycle["Percentage"] = (
            display_lifecycle["Percentage"]
            .map(lambda x: f"{x:.1%}")
        )

        st.dataframe(
            display_lifecycle,
            use_container_width=True,
            hide_index=True
        )


    # # =====================================================
    # # KEY CUSTOMER INSIGHTS
    # # =====================================================

    # st.write("### Key Customer Insights")

    # if (
    #     not customer_kpi_df.empty
    #     and not repeat_df.empty
    #     and not ltv_df.empty
    # ):

    #     insights = []

    #     # Repeat customer insight
    #     insights.append(
    #         f"**{repeat_rate:.1%}** of customers placed more than "
    #         f"one order during the selected period."
    #     )

    #     # Median vs average
    #     if median_customer_value > 0:

    #         value_ratio = (
    #             avg_customer_value /
    #             median_customer_value
    #         )

    #         if value_ratio > 1.5:
    #             insights.append(
    #                 "Average customer value is substantially above "
    #                 "the median, indicating that a smaller group of "
    #                 "higher-value customers is pulling the average upward."
    #             )

    #     # Revenue concentration
    #     if not concentration_df.empty:

    #         if top_10_pct >= 0.50:
    #             insights.append(
    #                 f"The top 10% of customers generated "
    #                 f"**{top_10_pct:.1%}** of revenue during the "
    #                 f"selected period."
    #             )
    #         else:
    #             insights.append(
    #                 f"The top 10% of customers generated "
    #                 f"**{top_10_pct:.1%}** of revenue during the "
    #                 f"selected period."
    #             )

    #     # Purchase frequency
    #     if not frequency_df.empty:

    #         one_order_row = frequency_df[
    #             frequency_df["purchase_frequency"] == "1 Order"
    #         ]

    #         if not one_order_row.empty:

    #             one_order_customers = int(
    #                 one_order_row.iloc[0]["customers"]
    #             )

    #             one_order_pct = (
    #                 one_order_customers /
    #                 total_customers
    #                 if total_customers > 0
    #                 else 0
    #             )

    #             insights.append(
    #                 f"**{one_order_pct:.1%}** of customers "
    #                 f"placed only one order during the selected period."
    #             )

    #     for insight in insights:
    #         st.markdown(f"- {insight}")

# =========================================================
# 🟡 TAB 4 — PRODUCTS
# =========================================================
with tab4:

    st.subheader("Product Insights")

    # st.caption(
    #     f"Product performance for "
    #     f"{start_date.strftime('%b %d, %Y')} – "
    #     f"{end_date.strftime('%b %d, %Y')}"
    # )

    # =====================================================
    # PRODUCT PERFORMANCE
    # =====================================================

    product_performance_query = f"""
        SELECT
            product_id,

            SUM(units_sold) AS units_sold,

            SUM(orders_with_product) AS orders,

            SUM(product_revenue) AS revenue,

            SUM(product_revenue)
                / NULLIF(SUM(units_sold), 0)
                AS realized_unit_price

        FROM marts.metrics_product_performance_daily

        {date_filter}

        GROUP BY product_id
    """

    performance_df = run_query(product_performance_query)

    # =====================================================
    # PRODUCT FUNNEL
    # =====================================================

    product_funnel_query = f"""
        SELECT
            product_id,

            SUM(product_views) AS product_views,

            SUM(add_to_cart_sessions)
                AS add_to_cart_sessions,

            SUM(checkout_sessions)
                AS checkout_sessions,

            SUM(purchase_sessions)
                AS purchase_sessions

        FROM marts.metrics_daily_product_funnel

        {date_filter}

        GROUP BY product_id
    """

    product_funnel_df = run_query(product_funnel_query)

    # =====================================================
    # COMBINE PRODUCT PERFORMANCE + FUNNEL
    # =====================================================

    if not performance_df.empty and not product_funnel_df.empty:

        product_df = performance_df.merge(
            product_funnel_df,
            on="product_id",
            how="outer"
        )

    elif not performance_df.empty:

        product_df = performance_df.copy()

        for column in [
            "product_views",
            "add_to_cart_sessions",
            "checkout_sessions",
            "purchase_sessions"
        ]:
            product_df[column] = 0

    elif not product_funnel_df.empty:

        product_df = product_funnel_df.copy()

        for column in [
            "units_sold",
            "orders",
            "revenue",
            "realized_unit_price"
        ]:
            product_df[column] = 0

    else:

        product_df = pd.DataFrame()

    # =====================================================
    # DERIVED PRODUCT METRICS
    # =====================================================

    if not product_df.empty:

        numeric_columns = [
            "units_sold",
            "orders",
            "revenue",
            "realized_unit_price",
            "product_views",
            "add_to_cart_sessions",
            "checkout_sessions",
            "purchase_sessions"
        ]

        for column in numeric_columns:
            if column in product_df.columns:
                product_df[column] = (
                    pd.to_numeric(
                        product_df[column],
                        errors="coerce"
                    )
                    .fillna(0)
                )

        product_df["cart_rate"] = (
            product_df["add_to_cart_sessions"]
            / product_df["product_views"].replace(0, pd.NA)
        )

        product_df["checkout_rate"] = (
            product_df["checkout_sessions"]
            / product_df["product_views"].replace(0, pd.NA)
        )

        product_df["purchase_rate"] = (
            product_df["purchase_sessions"]
            / product_df["product_views"].replace(0, pd.NA)
        )

        product_df["revenue_per_view"] = (
            product_df["revenue"]
            / product_df["product_views"].replace(0, pd.NA)
        )

        product_df["units_per_view"] = (
            product_df["units_sold"]
            / product_df["product_views"].replace(0, pd.NA)
        )

        product_df["revenue_per_order"] = (
            product_df["revenue"]
            / product_df["orders"].replace(0, pd.NA)
        )

        product_df["units_per_order"] = (
            product_df["units_sold"]
            / product_df["orders"].replace(0, pd.NA)
        )

        total_revenue = product_df["revenue"].sum()

        product_df["revenue_share"] = (
            product_df["revenue"]
            / total_revenue
            if total_revenue > 0
            else 0
        )

    # =====================================================
    # PRODUCT KPI SUMMARY
    # =====================================================

    st.write("### Product KPI Summary")

    if not product_df.empty:

        products = int(
            (
                product_df["units_sold"] > 0
            ).sum()
        )

        units_sold = int(
            product_df["units_sold"].sum()
        )

        product_revenue = float(
            product_df["revenue"].sum()
        )

        revenue_per_product = (
            product_revenue / products
            if products > 0
            else 0
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Products Sold",
            f"{products:,}"
        )

        col2.metric(
            "Units Sold",
            f"{units_sold:,}"
        )

        col3.metric(
            "Product Revenue",
            f"${product_revenue:,.0f}"
        )

        col4.metric(
            "Revenue / Product",
            f"${revenue_per_product:,.2f}"
        )

    else:

        st.info(
            "No product data available for the selected period."
        )

    # =====================================================
    # PRODUCT PERFORMANCE
    # =====================================================

    st.divider()

    st.write("### Product Performance")

    if not product_df.empty:

        top_col1, top_col2 = st.columns(2)

        # -------------------------------------------------
        # Revenue
        # -------------------------------------------------

        with top_col1:

            st.write("#### Top Products by Revenue")

            revenue_chart = (
                product_df[
                    [
                        "product_id",
                        "revenue"
                    ]
                ]
                .sort_values(
                    "revenue",
                    ascending=False
                )
                .head(10)
                .sort_values("revenue")
                .set_index("product_id")
            )

            st.bar_chart(
                revenue_chart,
                height=350
            )

        # -------------------------------------------------
        # Units
        # -------------------------------------------------

        with top_col2:

            st.write("#### Top Products by Units")

            units_chart = (
                product_df[
                    [
                        "product_id",
                        "units_sold"
                    ]
                ]
                .sort_values(
                    "units_sold",
                    ascending=False
                )
                .head(10)
                .sort_values("units_sold")
                .set_index("product_id")
            )

            st.bar_chart(
                units_chart,
                height=350
            )

        # -------------------------------------------------
        # Revenue vs Units
        # -------------------------------------------------

        st.write("#### Revenue vs. Unit Volume")

        matrix_df = product_df[
            [
                "product_id",
                "units_sold",
                "revenue"
            ]
        ].copy()

        st.scatter_chart(
            matrix_df,
            x="units_sold",
            y="revenue",
            size="revenue",
            color="product_id",
            height=400
        )

    # =====================================================
    # PRODUCT EFFICIENCY
    # =====================================================

    st.write("### Product Efficiency")

    if not product_df.empty:

        efficiency_col1, efficiency_col2, efficiency_col3, efficiency_col4 = (
            st.columns(4)
        )

        valid_views = product_df[
            product_df["product_views"] > 0
        ]

        overall_purchase_rate = (
            valid_views["purchase_sessions"].sum()
            / valid_views["product_views"].sum()
            if not valid_views.empty
            and valid_views["product_views"].sum() > 0
            else 0
        )

        overall_revenue_per_view = (
            valid_views["revenue"].sum()
            / valid_views["product_views"].sum()
            if not valid_views.empty
            and valid_views["product_views"].sum() > 0
            else 0
        )

        overall_units_per_view = (
            valid_views["units_sold"].sum()
            / valid_views["product_views"].sum()
            if not valid_views.empty
            and valid_views["product_views"].sum() > 0
            else 0
        )

        overall_revenue_per_order = (
            product_df["revenue"].sum()
            / product_df["orders"].sum()
            if product_df["orders"].sum() > 0
            else 0
        )

        efficiency_col1.metric(
            "View → Purchase",
            f"{overall_purchase_rate:.1%}"
        )

        efficiency_col2.metric(
            "Revenue / View",
            f"${overall_revenue_per_view:.2f}"
        )

        efficiency_col3.metric(
            "Units / View",
            f"{overall_units_per_view:.3f}"
        )

        efficiency_col4.metric(
            "Revenue / Order",
            f"${overall_revenue_per_order:,.2f}"
        )

    # =====================================================
    # PRODUCT REVENUE CONCENTRATION
    # =====================================================

    st.divider()

    st.write("### Product Revenue Concentration")

    if not product_df.empty:

        concentration_df = (
            product_df[
                [
                    "product_id",
                    "revenue"
                ]
            ]
            .sort_values(
                "revenue",
                ascending=False
            )
            .copy()
        )

        total_product_revenue = (
            concentration_df["revenue"].sum()
        )

        if total_product_revenue > 0:

            concentration_df["revenue_share"] = (
                concentration_df["revenue"]
                / total_product_revenue
            )

            concentration_df["cumulative_revenue_share"] = (
                concentration_df["revenue_share"]
                .cumsum()
            )

            product_count = len(
                concentration_df
            )

            top_10_count = max(
                1,
                int(product_count * 0.10)
            )

            top_20_count = max(
                1,
                int(product_count * 0.20)
            )

            top_50_count = max(
                1,
                int(product_count * 0.50)
            )

            top_10_pct = (
                concentration_df
                .head(top_10_count)["revenue"]
                .sum()
                / total_product_revenue
            )

            top_20_pct = (
                concentration_df
                .head(top_20_count)["revenue"]
                .sum()
                / total_product_revenue
            )

            top_50_pct = (
                concentration_df
                .head(top_50_count)["revenue"]
                .sum()
                / total_product_revenue
            )

            top_product_pct = (
                concentration_df.iloc[0]["revenue"]
                / total_product_revenue
            )

            col1, col2, col3, col4 = st.columns(4)

            col1.metric(
                "Top Product",
                f"{top_product_pct:.1%}"
            )

            col2.metric(
                "Top 10%",
                f"{top_10_pct:.1%}"
            )

            col3.metric(
                "Top 20%",
                f"{top_20_pct:.1%}"
            )

            col4.metric(
                "Top 50%",
                f"{top_50_pct:.1%}"
            )

            concentration_chart = (
                concentration_df[
                    ["cumulative_revenue_share"]
                ]
                .copy()
            )

            concentration_chart.index = range(
                1,
                len(concentration_chart) + 1
            )

            concentration_chart.columns = [
                "Cumulative Revenue Share"
            ]

            st.line_chart(
                concentration_chart,
                height=300
            )

    # =====================================================
    # PRODUCT FUNNEL
    # =====================================================

    st.divider()

    st.write("### Product Funnel")

    st.caption(
        "Product engagement and purchase behavior based on "
        "daily product-level clickstream metrics."
    )

    if not product_df.empty:

        total_views = (
            product_df["product_views"].sum()
        )

        total_cart = (
            product_df["add_to_cart_sessions"].sum()
        )

        total_checkout = (
            product_df["checkout_sessions"].sum()
        )

        total_purchase = (
            product_df["purchase_sessions"].sum()
        )

        funnel_df = pd.DataFrame({
            "Stage": [
                "Product Views",
                "Add to Cart",
                "Checkout",
                "Purchase"
            ],
            "Sessions": [
                total_views,
                total_cart,
                total_checkout,
                total_purchase
            ]
        })

        st.bar_chart(
            funnel_df.set_index("Stage"),
            height=300
        )

        funnel_col1, funnel_col2, funnel_col3 = st.columns(3)

        cart_rate = (
            total_cart / total_views
            if total_views > 0
            else 0
        )

        checkout_rate = (
            total_checkout / total_views
            if total_views > 0
            else 0
        )

        purchase_rate = (
            total_purchase / total_views
            if total_views > 0
            else 0
        )

        funnel_col1.metric(
            "View → Cart",
            f"{cart_rate:.1%}"
        )

        funnel_col2.metric(
            "View → Checkout",
            f"{checkout_rate:.1%}"
        )

        funnel_col3.metric(
            "View → Purchase",
            f"{purchase_rate:.1%}"
        )

        # -------------------------------------------------
        # Product Funnel Table
        # -------------------------------------------------

        st.write("#### Product Funnel Performance")

        funnel_table = product_df[
            [
                "product_id",
                "product_views",
                "add_to_cart_sessions",
                "checkout_sessions",
                "purchase_sessions",
                "cart_rate",
                "checkout_rate",
                "purchase_rate"
            ]
        ].copy()

        funnel_table = funnel_table.sort_values(
            "product_views",
            ascending=False
        )

        display_funnel = funnel_table.copy()

        display_funnel["cart_rate"] = (
            display_funnel["cart_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        display_funnel["checkout_rate"] = (
            display_funnel["checkout_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        display_funnel["purchase_rate"] = (
            display_funnel["purchase_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        display_funnel.columns = [
            "Product",
            "Views",
            "Add to Cart",
            "Checkout",
            "Purchases",
            "Cart Rate",
            "Checkout Rate",
            "Purchase Rate"
        ]

        st.dataframe(
            display_funnel,
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # PRODUCT PERFORMANCE OPPORTUNITIES
    # =====================================================

    st.write("### Product Performance Opportunities")

    if not product_df.empty:

        opportunity_df = product_df[
            product_df["product_views"] >= 10
        ].copy()

        if not opportunity_df.empty:

            median_purchase_rate = (
                opportunity_df["purchase_rate"]
                .dropna()
                .median()
            )

            # -------------------------------------------------
            # High Traffic / Low Conversion
            # -------------------------------------------------

            low_conversion = (
                opportunity_df[
                    opportunity_df["purchase_rate"]
                    < median_purchase_rate
                ]
                .sort_values(
                    "product_views",
                    ascending=False
                )
                .head(5)
            )

            if not low_conversion.empty:

                st.write(
                    "#### High Traffic / Low Conversion"
                )

                st.caption(
                    "Products with meaningful product traffic "
                    "but purchase conversion below the "
                    "product-level median."
                )

                opportunity_display = low_conversion[
                    [
                        "product_id",
                        "product_views",
                        "units_sold",
                        "revenue",
                        "cart_rate",
                        "purchase_rate"
                    ]
                ].copy()

                opportunity_display["revenue"] = (
                    opportunity_display["revenue"]
                    .map(lambda x: f"${x:,.2f}")
                )

                opportunity_display["cart_rate"] = (
                    opportunity_display["cart_rate"]
                    .map(
                        lambda x:
                        f"{x:.1%}" if pd.notna(x) else "—"
                    )
                )

                opportunity_display["purchase_rate"] = (
                    opportunity_display["purchase_rate"]
                    .map(
                        lambda x:
                        f"{x:.1%}" if pd.notna(x) else "—"
                    )
                )

                opportunity_display.columns = [
                    "Product",
                    "Views",
                    "Units Sold",
                    "Revenue",
                    "Cart Rate",
                    "Purchase Rate"
                ]

                st.dataframe(
                    opportunity_display,
                    use_container_width=True,
                    hide_index=True
                )

            # -------------------------------------------------
            # High Conversion / Lower Traffic
            # -------------------------------------------------

            high_conversion = (
                opportunity_df[
                    opportunity_df["purchase_rate"]
                    > median_purchase_rate
                ]
                .sort_values(
                    "product_views",
                    ascending=True
                )
                .head(5)
            )

            if not high_conversion.empty:

                st.write(
                    "#### Higher Conversion / Lower Traffic"
                )

                st.caption(
                    "Products with above-median purchase "
                    "conversion but comparatively lower "
                    "product traffic."
                )

                high_conversion_display = high_conversion[
                    [
                        "product_id",
                        "product_views",
                        "purchase_rate",
                        "revenue_per_view",
                        "revenue"
                    ]
                ].copy()

                high_conversion_display["purchase_rate"] = (
                    high_conversion_display["purchase_rate"]
                    .map(
                        lambda x:
                        f"{x:.1%}" if pd.notna(x) else "—"
                    )
                )

                high_conversion_display["revenue_per_view"] = (
                    high_conversion_display[
                        "revenue_per_view"
                    ]
                    .map(
                        lambda x:
                        f"${x:.2f}" if pd.notna(x) else "—"
                    )
                )

                high_conversion_display["revenue"] = (
                    high_conversion_display["revenue"]
                    .map(lambda x: f"${x:,.2f}")
                )

                high_conversion_display.columns = [
                    "Product",
                    "Views",
                    "Purchase Rate",
                    "Revenue / View",
                    "Revenue"
                ]

                st.dataframe(
                    high_conversion_display,
                    use_container_width=True,
                    hide_index=True
                )

    # =====================================================
    # PRODUCT PERFORMANCE TABLE
    # =====================================================

    st.divider()

    st.write("### Product Performance Table")

    if not product_df.empty:

        display_df = product_df[
            [
                "product_id",
                "product_views",
                "units_sold",
                "orders",
                "revenue",
                "revenue_share",
                "purchase_rate",
                "revenue_per_view",
                "realized_unit_price",
                "revenue_per_order",
                "units_per_order"
            ]
        ].copy()

        display_df = display_df.sort_values(
            "revenue",
            ascending=False
        )

        display_df["revenue"] = (
            display_df["revenue"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df["revenue_share"] = (
            display_df["revenue_share"]
            .map(lambda x: f"{x:.1%}")
        )

        display_df["purchase_rate"] = (
            display_df["purchase_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        display_df["revenue_per_view"] = (
            display_df["revenue_per_view"]
            .map(
                lambda x:
                f"${x:.2f}" if pd.notna(x) else "—"
            )
        )

        display_df["realized_unit_price"] = (
            display_df["realized_unit_price"]
            .map(
                lambda x:
                f"${x:.2f}" if pd.notna(x) else "—"
            )
        )

        display_df["revenue_per_order"] = (
            display_df["revenue_per_order"]
            .map(
                lambda x:
                f"${x:,.2f}" if pd.notna(x) else "—"
            )
        )

        display_df["units_per_order"] = (
            display_df["units_per_order"]
            .map(
                lambda x:
                f"{x:.2f}" if pd.notna(x) else "—"
            )
        )

        display_df.columns = [
            "Product",
            "Views",
            "Units Sold",
            "Orders",
            "Revenue",
            "Revenue Share",
            "Purchase Rate",
            "Revenue / View",
            "Realized Unit Price",
            "Revenue / Order",
            "Units / Order"
        ]

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # KEY PRODUCT INSIGHTS
    # =====================================================

    st.write("### Key Product Insights")

    insights = []

    if not product_df.empty:

        # -------------------------------------------------
        # Top Revenue Product
        # -------------------------------------------------

        top_revenue_product = (
            product_df
            .sort_values(
                "revenue",
                ascending=False
            )
            .iloc[0]
        )

        insights.append(
            f"Product **{top_revenue_product['product_id']}** "
            f"generated the most revenue during the selected "
            f"period: **${top_revenue_product['revenue']:,.2f}**."
        )

        # -------------------------------------------------
        # Top Units Product
        # -------------------------------------------------

        top_units_product = (
            product_df
            .sort_values(
                "units_sold",
                ascending=False
            )
            .iloc[0]
        )

        insights.append(
            f"Product **{top_units_product['product_id']}** "
            f"had the highest unit volume with "
            f"**{int(top_units_product['units_sold']):,} units sold**."
        )

        # -------------------------------------------------
        # Revenue Concentration
        # -------------------------------------------------

        if total_product_revenue > 0:

            insights.append(
                f"The top 10% of products generated "
                f"**{top_10_pct:.1%}** of product revenue."
            )

        # -------------------------------------------------
        # Overall Funnel
        # -------------------------------------------------

        if total_views > 0:

            insights.append(
                f"Across tracked product sessions, the overall "
                f"view-to-purchase conversion rate was "
                f"**{purchase_rate:.1%}**."
            )

        # -------------------------------------------------
        # Highest Revenue / View
        # -------------------------------------------------

        revenue_view_df = product_df[
            product_df["product_views"] > 0
        ].copy()

        if not revenue_view_df.empty:

            best_revenue_view = (
                revenue_view_df
                .sort_values(
                    "revenue_per_view",
                    ascending=False
                )
                .iloc[0]
            )

            insights.append(
                f"Product **{best_revenue_view['product_id']}** "
                f"generated the highest revenue per product view "
                f"at **${best_revenue_view['revenue_per_view']:.2f}**."
            )

    if insights:

        for insight in insights:
            st.markdown(f"- {insight}")


# ----------------------------
# FOOTER
# ----------------------------
st.divider()

st.caption(
    f"Last updated: "
    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
