from tta_helpers import with_gc
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64
import numpy as np
import datetime as dt


@with_gc
def get_correlation_matrix(results):
    # Create a DataFrame with daily PnL for each strategy
    pnl_data = {strategy: df["Day PnL"] for strategy, df in results.items()}
    pnl_df = pd.DataFrame(pnl_data)

    # Calculate the correlation matrix
    corr_matrix = pnl_df.corr()

    # Create a heatmap
    plt.figure(figsize=(12, 6))
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1, center=0)
    plt.title("Strategy Correlation Matrix")

    # Rotate x-axis labels
    plt.xticks(rotation=30, ha="right")

    # Rotate y-axis labels
    plt.yticks(rotation=0, ha="right")

    # Adjust layout to prevent cutting off labels
    plt.tight_layout()

    # Save the plot to a buffer
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)

    # Convert the image to base64
    img_str = base64.b64encode(buf.getvalue())
    plt.close()

    return img_str


@with_gc
def get_monthly_pnl_chart(results):
    plt.figure(figsize=(12, 6))

    # Get all unique months across all strategies
    all_months = set()
    for df in results.values():
        df["Date"] = pd.to_datetime(df["Date"])
        all_months.update(df["Date"].dt.to_period("M"))
    all_months = sorted(list(all_months))

    # Set up the x-axis
    x = np.arange(len(all_months))
    width = 0.8 / len(results)  # Adjust bar width based on number of strategies

    # Plot bars for each strategy
    for i, (strategy, df) in enumerate(results.items()):
        monthly_pnl = df.groupby(df["Date"].dt.to_period("M"))["Day PnL"].sum()

        # Align the strategy's data with all_months
        pnl_values = [monthly_pnl.get(month, 0) for month in all_months]

        plt.bar(x + i * width, pnl_values, width, label=strategy, alpha=0.8)

    plt.title("Monthly PnL")
    plt.xlabel("Month")
    plt.ylabel("PnL")
    plt.legend()

    # Set x-axis ticks
    plt.xticks(
        x + width * (len(results) - 1) / 2,
        [m.strftime("%Y-%m") for m in all_months],
        rotation=45,
        ha="right",
    )

    plt.tight_layout()
    # buffer for saving data
    buf = BytesIO()
    plt.savefig(buf, dpi=150, bbox_inches="tight")
    buf.seek(0)
    # Convert PNG to base64 string
    img_str = base64.b64encode(buf.getvalue())
    plt.close()
    return img_str


@with_gc
def get_news_event_pnl_chart(results, news_events: dict, sum=True):
    # Get list of news events
    events = list(news_events.keys())

    # Initialize a dictionary to hold summed PnL for each strategy and news event
    summed_pnls = {
        strategy: {event: 0 for event in events} for strategy in results.keys()
    }

    # Sum the PnL values for each strategy and news event
    for strategy, df in results.items():
        for event, dates in news_events.items():
            event_dates = pd.to_datetime(dates)
            if sum:
                event_pnl = df[df["Date"].dt.date.isin(event_dates.date)][
                    "Day PnL"
                ].sum()
            else:
                event_pnl = df[df["Date"].dt.date.isin(event_dates.date)][
                    "Day PnL"
                ].mean()
            summed_pnls[strategy][event] = event_pnl

    # Prepare data for the bar chart
    x = np.arange(len(events))  # the label locations
    width = 0.8 / len(results)
    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot bars for each strategy
    for i, (strategy, pnl_dict) in enumerate(summed_pnls.items()):
        pnls = [pnl_dict[event] for event in events]
        ax.bar(x + (i - (len(results) - 1) / 2) * width, pnls, width, label=strategy)

    # Add labels, title, and custom x-axis tick labels
    if sum:
        ax.set_ylabel("Total PnL")
        ax.set_title("PnL by News Event")
    else:
        ax.set_ylabel("Average PnL")
        ax.set_title("Average PnL Per News Event")
    ax.set_xticks(x)
    ax.set_xticklabels(events, rotation=45, ha="right")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.25), ncol=min(len(results), 4))

    fig.subplots_adjust(bottom=0.3)
    plt.tight_layout()
    # buffer for saving data
    buf = BytesIO()
    plt.savefig(buf, dpi=150, bbox_inches="tight")
    buf.seek(0)
    # Convert PNG to base64 string
    img_str = base64.b64encode(buf.getvalue())
    plt.close()
    return img_str


@with_gc
def get_pnl_plot(results):
    table_data = []
    plt.figure(figsize=(8, 4))
    for strategy, df in results.items():
        plt.plot(df["Date"], df["Current Value"], label=strategy)
        # Calculate summary statistics for the strategy
        final_value = df["Current Value"].iloc[-1]
        max_dd = df["Max DD"].max()
        dd_days = df["DD Days"].max()
        initial_value = df["Initial Value"].min()
        total_return = (final_value - initial_value) / initial_value
        win_streak = df["Win Streak"].max()
        loss_streak = df["Loss Streak"].max()
        # CAGR
        start_dt = df["Date"].iloc[0]
        end_dt = df["Date"].iloc[-1]
        years = (end_dt - start_dt).days / 365.25
        cagr = ((final_value / initial_value) ** (1 / years)) - 1

        # Sharpe Ratio
        df["Daily Return"] = df["Current Value"].pct_change()
        std_dev = df["Daily Return"].std()
        risk_free_rate = 0.02 / 252  # Assume 2% annual risk-free rate, convert to daily
        excess_returns = df["Daily Return"] - risk_free_rate
        sharpe_ratio = np.sqrt(252) * excess_returns.mean() / std_dev  # Annualized

        if max_dd:
            mar = cagr / max_dd
        else:
            mar = float("inf")

        # Group PnL by month
        df["YearMonth"] = df["Date"].dt.to_period("M")
        monthly_pnl = df.groupby("YearMonth")["Day PnL"].sum()

        # Calculate largest and lowest monthly PnL with their corresponding dates
        largest_monthly_pnl = monthly_pnl.max()
        largest_monthly_pnl_date = monthly_pnl.idxmax().to_timestamp()
        lowest_monthly_pnl = monthly_pnl.min()
        lowest_monthly_pnl_date = monthly_pnl.idxmin().to_timestamp()

        # Format the date strings
        largest_monthly_pnl_str = (
            f"{largest_monthly_pnl:,.2f} {largest_monthly_pnl_date.strftime('%b%y')}"
        )
        lowest_monthly_pnl_str = (
            f"{lowest_monthly_pnl:,.2f} {lowest_monthly_pnl_date.strftime('%b%y')}"
        )

        # Create row for Table
        row_data = [
            f"{strategy}",
            f"{final_value:,.2f}",
            f"{(final_value - initial_value):,.2f}",
            f"{total_return:,.2%}",
            f"{cagr:.2%}",
            f"{max_dd:.2%}",
            f"{dd_days}",
            f"{win_streak}",
            f"{loss_streak}",
            largest_monthly_pnl_str,
            lowest_monthly_pnl_str,
            f"{mar:.2f}",
            f"{sharpe_ratio:.2f}",
        ]

        table_data.append(row_data)

    plt.title("P/L Walk Forward Test")
    plt.xlabel("Date")
    plt.ylabel("Current Value")
    plt.legend()
    plt.grid(True)
    # plt.xticks(rotation=45)
    plt.tight_layout()

    # buffer for saving data
    buf = BytesIO()
    plt.savefig(buf, dpi=150)
    buf.seek(0)
    # Convert PNG to base64 string
    img_str = base64.b64encode(buf.getvalue())
    plt.close()
    return table_data, img_str


@with_gc
def get_weekday_pnl_chart(results, weekday_list):
    # Filter weekdays based on exclusions
    weekdays = [day[:3] for day in weekday_list]

    # Initialize a dictionary to hold summed PnL for each strategy and weekday
    summed_pnls = {
        strategy: {day: 0 for day in weekdays} for strategy in results.keys()
    }

    # Sum the PnL values for each strategy and weekday
    for strategy, df in results.items():
        for day in weekdays:
            summed_pnls[strategy][day] = df[df["Weekday"] == day]["Day PnL"].sum()

    # Prepare data for the bar chart
    x = np.arange(len(weekdays))  # the label locations
    width = 0.8 / len(results)

    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot bars for each strategy
    for i, (strategy, pnl_dict) in enumerate(summed_pnls.items()):
        pnls = [pnl_dict[day] for day in weekdays]
        ax.bar(x + (i - (len(results) - 1) / 2) * width, pnls, width, label=strategy)

    # Add labels, title, and custom x-axis tick labels
    ax.set_ylabel("Total PnL")
    ax.set_title("PnL by Weekday")
    ax.set_xticks(x)
    ax.set_xticklabels(weekdays)
    ax.legend(
        loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=min(len(results), 4)
    )
    fig.subplots_adjust(bottom=0.2)
    plt.tight_layout()
    # buffer for saving data
    buf = BytesIO()
    plt.savefig(buf, dpi=150, bbox_inches="tight")
    buf.seek(0)
    # Convert PNG to base64 string
    img_str = base64.b64encode(buf.getvalue())
    plt.close()
    return img_str
