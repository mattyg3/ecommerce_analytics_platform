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
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
"""


# ----------------------------
# TITLE
# ----------------------------
st.title("🛒 Ecommerce Analytics Dashboard")

if min_date and max_date:
    st.markdown(
        f"**Data coverage:** "
        f"{min_date.strftime('%b %d, %Y')} – "
        f"{max_date.strftime('%b %d, %Y')}"
    )

st.markdown(
        f"**Data coverage:** "
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

    # -----------------------------------------------------
    # PERIOD COMPARISON
    # -----------------------------------------------------
    previous_start, previous_end = get_valid_previous_period(
        start_date,
        end_date
    )

    if previous_start is not None:
        previous_date_filter = f"""
            WHERE order_date BETWEEN '{previous_start}' AND '{previous_end}'
        """
    else:
        previous_date_filter = None


    # -----------------------------------------------------
    # CURRENT PERIOD KPIs
    # -----------------------------------------------------
    kpi_query = f"""
        SELECT
            COALESCE(SUM(order_total_amount), 0) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(DISTINCT user_id) AS customers
        FROM marts.fact_orders
        {date_filter}
    """

    kpis = run_query(kpi_query)


    # -----------------------------------------------------
    # PREVIOUS PERIOD KPIs
    # -----------------------------------------------------
    if previous_date_filter is not None:
        previous_kpi_query = f"""
            SELECT
                COALESCE(SUM(order_total_amount), 0) AS revenue,
                COUNT(DISTINCT order_id) AS orders,
                COUNT(DISTINCT user_id) AS customers
            FROM marts.fact_orders
            {previous_date_filter}
        """

        previous_kpis = run_query(previous_kpi_query)
    else:
        previous_kpis = pd.DataFrame()


    if not kpis.empty:

        revenue = float(kpis.iloc[0]["revenue"] or 0)
        orders = int(kpis.iloc[0]["orders"] or 0)
        customers = int(kpis.iloc[0]["customers"] or 0)

        aov = revenue / orders if orders else 0


        if not previous_kpis.empty:

            previous_revenue = float(
                previous_kpis.iloc[0]["revenue"] or 0
            )

            previous_orders = int(
                previous_kpis.iloc[0]["orders"] or 0
            )

            previous_customers = int(
                previous_kpis.iloc[0]["customers"] or 0
            )

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

    time_expression = get_time_expression(
        granularity
    )

    trend_query = f"""
        SELECT
            {time_expression} AS period,
            SUM(order_total_amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders
        FROM marts.fact_orders
        {date_filter}
        GROUP BY 1
        ORDER BY 1
    """

    trend_df = run_query(trend_query)


    if not trend_df.empty:

        trend_df["period"] = pd.to_datetime(
            trend_df["period"]
        )

        trend_df = trend_df.set_index("period")

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
        st.info("No revenue data available for this period.")


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

        dow_query = f"""
            SELECT
                DAYNAME(order_ts) AS day_name,
                DAYOFWEEK(order_ts) AS day_number,
                SUM(order_total_amount) AS revenue,
                COUNT(DISTINCT order_id) AS orders
            FROM marts.fact_orders
            {date_filter}
            GROUP BY 1, 2
            ORDER BY day_number
        """

        dow_df = run_query(dow_query)

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

            dow_df = dow_df.sort_values("day_name")

            chart_df = dow_df[
                ["day_name", "revenue"]
            ].set_index("day_name")

            st.bar_chart(
                chart_df,
                height=300
            )

        else:
            st.info("No daily revenue data available.")


    # -----------------------------------------------------
    # ORDER METRICS
    # -----------------------------------------------------
    with right_col:

        st.markdown("#### Order Metrics")

        order_metrics_query = f"""
            SELECT
                COUNT(DISTINCT order_id) AS orders,
                COUNT(DISTINCT order_date) AS active_days,
                AVG(order_total_amount) AS avg_order_value,
                MEDIAN(order_total_amount) AS median_order_value
            FROM marts.fact_orders
            {date_filter}
        """

        order_metrics_df = run_query(
            order_metrics_query
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
                f"${float(om['median_order_value'] or 0):,.2f}"
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
            {date_filter}
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

        customer_mix_df["percentage"] = (
            customer_mix_df["revenue"]
            / total_mix_revenue
        )

        mix_col1, mix_col2 = st.columns([1.5, 1])

        with mix_col1:

            chart_df = customer_mix_df[
                ["customer_type", "revenue"]
            ].set_index("customer_type")

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


    # Revenue insight
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


    # AOV insight
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


    # Peak day insight
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


    # Customer mix insight
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


    if insights:

        for insight in insights:
            st.markdown(f"• {insight}")

    else:
        st.info("Not enough data to generate insights.")


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
        WITH session_events AS (

            SELECT
                session_id,

                MAX(
                    CASE
                        WHEN event_type = 'view_product'
                        THEN 1 ELSE 0
                    END
                ) AS product_view,

                MAX(
                    CASE
                        WHEN event_type = 'add_to_cart'
                        THEN 1 ELSE 0
                    END
                ) AS add_to_cart,

                MAX(
                    CASE
                        WHEN event_type = 'checkout_start'
                        THEN 1 ELSE 0
                    END
                ) AS checkout_start,

                MAX(
                    CASE
                        WHEN event_type = 'purchase'
                        THEN 1 ELSE 0
                    END
                ) AS purchase

            FROM bronze.clickstream

            WHERE event_time >= TIMESTAMP '{start_date} 00:00:00'
            AND event_time < TIMESTAMP '{end_date}' + INTERVAL '1 day'

            GROUP BY session_id
        )

        SELECT
            COUNT(*) AS sessions,
            SUM(product_view) AS product_views,
            SUM(add_to_cart) AS add_to_carts,
            SUM(checkout_start) AS checkouts,
            SUM(purchase) AS purchases

        FROM session_events
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

    # st.caption(
    #     f"Customer behavior for "
    #     f"{start_date.strftime('%b %d, %Y')} – "
    #     f"{end_date.strftime('%b %d, %Y')}"
    # )


    # =====================================================
    # CUSTOMER KPI SUMMARY
    # =====================================================

    customer_kpi_query = f"""
        WITH customer_orders AS (
            SELECT
                user_id,
                COUNT(DISTINCT order_id) AS order_count,
                SUM(order_total_amount) AS revenue
            FROM marts.fact_orders
            {date_filter}
            GROUP BY user_id
        )

        SELECT
            COUNT(*) AS customers,
            SUM(revenue) AS revenue,
            SUM(order_count) AS orders,
            AVG(revenue) AS avg_customer_value,
            MEDIAN(revenue) AS median_customer_value,
            COUNT(*) FILTER (
                WHERE order_count > 1
            ) AS repeat_customers
        FROM customer_orders
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
            SUM(order_total_amount) AS customer_value
        FROM marts.fact_orders
        {date_filter}
        GROUP BY user_id
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
        WITH customer_orders AS (
            SELECT
                user_id,
                COUNT(DISTINCT order_id) AS order_count,
                SUM(order_total_amount) AS revenue
            FROM marts.fact_orders
            {date_filter}
            GROUP BY user_id
        )

        SELECT
            CASE
                WHEN order_count = 1
                    THEN 'One-Time'
                ELSE 'Repeat'
            END AS customer_type,

            COUNT(*) AS customers,
            SUM(revenue) AS revenue

        FROM customer_orders

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
        WITH customer_orders AS (
            SELECT
                user_id,
                COUNT(DISTINCT order_id) AS order_count
            FROM marts.fact_orders
            {date_filter}
            GROUP BY user_id
        )

        SELECT
            CASE
                WHEN order_count >= 4 THEN '4+ Orders'
                ELSE CAST(order_count AS VARCHAR) || ' Order'
                    || CASE
                        WHEN order_count = 1 THEN ''
                        ELSE 's'
                       END
            END AS purchase_frequency,

            CASE
                WHEN order_count >= 4 THEN 4
                ELSE order_count
            END AS sort_order,

            COUNT(*) AS customers

        FROM customer_orders

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
    # CUSTOMER REVENUE CONCENTRATION
    # =====================================================

    st.write("### Customer Revenue Concentration")

    concentration_query = f"""
        WITH customer_revenue AS (
            SELECT
                user_id,
                SUM(order_total_amount) AS revenue
            FROM marts.fact_orders
            {date_filter}
            GROUP BY user_id
        ),

        ranked_customers AS (
            SELECT
                user_id,
                revenue,
                SUM(revenue) OVER (
                    ORDER BY revenue DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
                ) AS cumulative_revenue,

                SUM(revenue) OVER () AS total_revenue

            FROM customer_revenue
        )

        SELECT
            user_id,
            revenue,
            cumulative_revenue / NULLIF(total_revenue, 0)
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
            COUNT(DISTINCT order_id) AS orders,
            SUM(order_total_amount) AS revenue,
            AVG(order_total_amount) AS avg_order_value,
            MIN(order_ts) AS first_order,
            MAX(order_ts) AS last_order
        FROM marts.fact_orders
        {date_filter}
        GROUP BY user_id
        ORDER BY revenue DESC
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


    # =====================================================
    # KEY CUSTOMER INSIGHTS
    # =====================================================

    st.write("### Key Customer Insights")

    if (
        not customer_kpi_df.empty
        and not repeat_df.empty
        and not ltv_df.empty
    ):

        insights = []

        # Repeat customer insight
        insights.append(
            f"**{repeat_rate:.1%}** of customers placed more than "
            f"one order during the selected period."
        )

        # Median vs average
        if median_customer_value > 0:

            value_ratio = (
                avg_customer_value /
                median_customer_value
            )

            if value_ratio > 1.5:
                insights.append(
                    "Average customer value is substantially above "
                    "the median, indicating that a smaller group of "
                    "higher-value customers is pulling the average upward."
                )

        # Revenue concentration
        if not concentration_df.empty:

            if top_10_pct >= 0.50:
                insights.append(
                    f"The top 10% of customers generated "
                    f"**{top_10_pct:.1%}** of revenue during the "
                    f"selected period."
                )
            else:
                insights.append(
                    f"The top 10% of customers generated "
                    f"**{top_10_pct:.1%}** of revenue during the "
                    f"selected period."
                )

        # Purchase frequency
        if not frequency_df.empty:

            one_order_row = frequency_df[
                frequency_df["purchase_frequency"] == "1 Order"
            ]

            if not one_order_row.empty:

                one_order_customers = int(
                    one_order_row.iloc[0]["customers"]
                )

                one_order_pct = (
                    one_order_customers /
                    total_customers
                    if total_customers > 0
                    else 0
                )

                insights.append(
                    f"**{one_order_pct:.1%}** of customers "
                    f"placed only one order during the selected period."
                )

        for insight in insights:
            st.markdown(f"- {insight}")

# =========================================================
# 🟡 TAB 4 — PRODUCTS
# =========================================================
with tab4:

    st.subheader("Product Insights")

    st.caption(
        f"Product performance for "
        f"{start_date.strftime('%b %d, %Y')} – "
        f"{end_date.strftime('%b %d, %Y')}"
    )

    # =====================================================
    # PRODUCT KPI SUMMARY
    # =====================================================

    product_kpi_query = f"""
        SELECT
            COUNT(DISTINCT product_id) AS products,
            COUNT(DISTINCT order_id) AS orders,
            SUM(quantity) AS units_sold,
            SUM(line_amount) AS revenue,
            AVG(price) AS avg_unit_price
        FROM marts.fact_order_items
        {date_filter}
    """

    product_kpi_df = run_query(product_kpi_query)

    if not product_kpi_df.empty:

        row = product_kpi_df.iloc[0]

        product_count = int(row["products"] or 0)
        product_orders = int(row["orders"] or 0)
        units_sold = int(row["units_sold"] or 0)
        product_revenue = float(row["revenue"] or 0)
        avg_unit_price = float(row["avg_unit_price"] or 0)

        revenue_per_product = (
            product_revenue / product_count
            if product_count > 0
            else 0
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Products Sold",
            f"{product_count:,}"
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
            "Avg Unit Price",
            f"${avg_unit_price:,.2f}"
        )


    st.divider()


    # =====================================================
    # TOP PRODUCTS
    # =====================================================

    st.write("### Top Products")

    top_products_query = f"""
        SELECT
            product_id,
            SUM(quantity) AS units_sold,
            SUM(line_amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            AVG(price) AS avg_price
        FROM marts.fact_order_items
        {date_filter}
        GROUP BY product_id
        ORDER BY revenue DESC
        LIMIT 10
    """

    top_products_df = run_query(
        top_products_query
    )

    if not top_products_df.empty:

        top_col1, top_col2 = st.columns(2)

        with top_col1:

            st.write("#### Revenue")

            revenue_chart = (
                top_products_df[
                    ["product_id", "revenue"]
                ]
                .set_index("product_id")
                .sort_values("revenue")
            )

            st.bar_chart(
                revenue_chart,
                height=350
            )

        with top_col2:

            st.write("#### Units Sold")

            units_chart = (
                top_products_df[
                    ["product_id", "units_sold"]
                ]
                .set_index("product_id")
                .sort_values("units_sold")
            )

            st.bar_chart(
                units_chart,
                height=350
            )


    # =====================================================
    # PRODUCT REVENUE CONCENTRATION
    # =====================================================

    st.write("### Product Revenue Concentration")

    concentration_query = f"""
        WITH product_revenue AS (

            SELECT
                product_id,
                SUM(line_amount) AS revenue
            FROM marts.fact_order_items
            {date_filter}
            GROUP BY product_id
        ),

        ranked_products AS (

            SELECT
                product_id,
                revenue,

                SUM(revenue) OVER (
                    ORDER BY revenue DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
                ) AS cumulative_revenue,

                SUM(revenue) OVER () AS total_revenue

            FROM product_revenue
        )

        SELECT
            product_id,
            revenue,
            cumulative_revenue
                / NULLIF(total_revenue, 0)
                AS cumulative_revenue_pct

        FROM ranked_products

        ORDER BY revenue DESC
    """

    concentration_df = run_query(
        concentration_query
    )

    if not concentration_df.empty:

        total_product_revenue = (
            concentration_df["revenue"].sum()
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
            top_10_revenue / total_product_revenue
            if total_product_revenue > 0
            else 0
        )

        top_product_pct = (
            concentration_df.iloc[0]["revenue"]
            / total_product_revenue
            if total_product_revenue > 0
            else 0
        )

        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Top 10% of Products",
                f"{top_10_pct:.1%} of Revenue"
            )

        with col2:
            st.metric(
                "Top Product",
                f"{top_product_pct:.1%} of Revenue"
            )

        concentration_chart = (
            concentration_df[
                ["cumulative_revenue_pct"]
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
    # PRODUCT PERFORMANCE TABLE
    # =====================================================

    st.write("### Product Performance")

    performance_query = f"""
        SELECT
            product_id,

            SUM(quantity)
                AS units_sold,

            COUNT(DISTINCT order_id)
                AS orders,

            SUM(line_amount)
                AS revenue,

            AVG(price)
                AS avg_unit_price,

            SUM(line_amount)
                / NULLIF(SUM(quantity), 0)
                AS realized_unit_price

        FROM marts.fact_order_items

        {date_filter}

        GROUP BY product_id

        ORDER BY revenue DESC
    """

    performance_df = run_query(
        performance_query
    )

    if not performance_df.empty:

        display_df = performance_df.copy()

        display_df["revenue"] = (
            display_df["revenue"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df["avg_unit_price"] = (
            display_df["avg_unit_price"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df["realized_unit_price"] = (
            display_df["realized_unit_price"]
            .map(lambda x: f"${x:,.2f}")
        )

        display_df.columns = [
            "Product",
            "Units Sold",
            "Orders",
            "Revenue",
            "Avg Unit Price",
            "Realized Unit Price"
        ]

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )


    # =====================================================
    # PRODUCT FUNNEL
    # =====================================================

    st.divider()

    st.write("### Product Funnel")

    st.caption(
        "Product engagement and purchase behavior based on "
        "clickstream activity."
    )

    product_funnel_query = f"""
        SELECT
            product_id,

            COUNT(DISTINCT CASE
                WHEN event_type = 'view_product'
                THEN session_id
            END) AS product_views,

            COUNT(DISTINCT CASE
                WHEN event_type = 'add_to_cart'
                THEN session_id
            END) AS add_to_cart_sessions,

            COUNT(DISTINCT CASE
                WHEN event_type = 'checkout_start'
                THEN session_id
            END) AS checkout_sessions,

            COUNT(DISTINCT CASE
                WHEN event_type = 'purchase'
                THEN session_id
            END) AS purchase_sessions

        FROM bronze.clickstream

        WHERE event_time >= TIMESTAMP '{start_date} 00:00:00'
          AND event_time < TIMESTAMP '{end_date}'
                + INTERVAL '1 day'

          AND product_id IS NOT NULL

        GROUP BY product_id

        ORDER BY product_views DESC
    """

    product_funnel_df = run_query(
        product_funnel_query
    )

    if not product_funnel_df.empty:

        product_funnel_df["cart_rate"] = (
            product_funnel_df["add_to_cart_sessions"]
            / product_funnel_df["product_views"]
            .replace(0, pd.NA)
        )

        product_funnel_df["checkout_rate"] = (
            product_funnel_df["checkout_sessions"]
            / product_funnel_df["product_views"]
            .replace(0, pd.NA)
        )

        product_funnel_df["purchase_rate"] = (
            product_funnel_df["purchase_sessions"]
            / product_funnel_df["product_views"]
            .replace(0, pd.NA)
        )

        funnel_display = product_funnel_df.copy()

        funnel_display["cart_rate"] = (
            funnel_display["cart_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        funnel_display["checkout_rate"] = (
            funnel_display["checkout_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        funnel_display["purchase_rate"] = (
            funnel_display["purchase_rate"]
            .map(
                lambda x:
                f"{x:.1%}" if pd.notna(x) else "—"
            )
        )

        funnel_display.columns = [
            "Product",
            "Product Views",
            "Add to Cart",
            "Checkout",
            "Purchases",
            "Cart Rate",
            "Checkout Rate",
            "Purchase Rate"
        ]

        st.dataframe(
            funnel_display,
            use_container_width=True,
            hide_index=True
        )


    # =====================================================
    # PRODUCT PERFORMANCE OPPORTUNITIES
    # =====================================================

    st.write("### Product Performance Opportunities")

    if (
        not product_funnel_df.empty
        and not performance_df.empty
    ):

        opportunity_df = product_funnel_df.merge(
            performance_df,
            on="product_id",
            how="left"
        )

        opportunity_df = opportunity_df[
            opportunity_df["product_views"] >= 10
        ].copy()

        if not opportunity_df.empty:

            opportunity_df["purchase_rate"] = (
                opportunity_df["purchase_sessions"]
                / opportunity_df["product_views"]
            )

            opportunity_df["cart_rate"] = (
                opportunity_df["add_to_cart_sessions"]
                / opportunity_df["product_views"]
            )

            median_purchase_rate = (
                opportunity_df["purchase_rate"]
                .median()
            )

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

                st.caption(
                    "Products receiving meaningful traffic but "
                    "converting below the product median."
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
                    .map(lambda x: f"{x:.1%}")
                )

                opportunity_display["purchase_rate"] = (
                    opportunity_display["purchase_rate"]
                    .map(lambda x: f"{x:.1%}")
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


    # =====================================================
    # KEY PRODUCT INSIGHTS
    # =====================================================

    st.write("### Key Product Insights")

    insights = []

    if not top_products_df.empty:

        top_product = top_products_df.iloc[0]

        insights.append(
            f"Product **{top_product['product_id']}** generated "
            f"the most revenue during the selected period: "
            f"**${top_product['revenue']:,.2f}**."
        )

    if not concentration_df.empty:

        insights.append(
            f"The top 10% of products generated "
            f"**{top_10_pct:.1%}** of product revenue."
        )

    if not top_products_df.empty:

        top_units_product = (
            top_products_df
            .sort_values(
                "units_sold",
                ascending=False
            )
            .iloc[0]
        )

        insights.append(
            f"Product **{top_units_product['product_id']}** "
            f"had the highest unit volume with "
            f"**{int(top_units_product['units_sold']):,} units sold."
        )

    if not product_funnel_df.empty:

        total_views = (
            product_funnel_df["product_views"].sum()
        )

        total_purchases = (
            product_funnel_df["purchase_sessions"].sum()
        )

        overall_conversion = (
            total_purchases / total_views
            if total_views > 0
            else 0
        )

        insights.append(
            f"Across tracked product sessions, the overall "
            f"view-to-purchase conversion rate was "
            f"**{overall_conversion:.1%}**."
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
