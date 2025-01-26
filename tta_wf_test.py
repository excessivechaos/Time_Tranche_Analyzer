from multiprocessing import Queue
from tta_helpers import (
    setup_logging,
    with_gc,
    is_BYOB_data,
    get_spx_gaps,
    get_top_times,
    get_next_filename,
)
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import uuid
from loguru import logger
from optimizer_result_model import OptimizerResult
import pickle


def export_oo_sig_file(trade_log_df: pd.DataFrame, filename: str):
    """
    Takes a trade log df and converts to an
    Option Omega signal file that can be loaded
    into OO for backtesting and adding to OO portfolio
    """
    signal_data = []
    for _, trade in trade_log_df.iterrows():
        if "Legs" in trade and isinstance(trade["Legs"], str):
            # OO data processing (unchanged)
            legs = trade["Legs"].split("|")
            for leg in legs:
                leg_parts = leg.strip().split(" ")
                signal_data.append(
                    {
                        "OPEN_DATETIME": trade["Date Opened"].strftime("%Y-%m-%d")
                        + " "
                        + trade["Time Opened"][:5],
                        "BUY_SELL": "B" if leg_parts[5] == "BTO" else "S",
                        "CALL_PUT": leg_parts[4],
                        "STRIKE": leg_parts[3],
                        "EXPIRATION": trade["Date Opened"].strftime("%Y-%m-%d"),
                        "QUANTITY": int(leg_parts[0]) * trade["qty"],
                    }
                )
        else:
            # BYOB data processing
            open_datetime = trade["EntryTime"].strftime("%Y-%m-%d %H:%M")

            # Handle close datetime
            if pd.notnull(trade["CloseDate"]) and pd.notnull(trade["CloseTime"]):
                close_datetime = f"{trade['CloseDate']} {trade['CloseTime'][:5]}"
            else:
                # Use OpenDate and set time to 16:00 if CloseDate or CloseTime is missing
                close_datetime = f"{trade['OpenDate']} 16:00"

            signal_data.append(
                {
                    "OPEN_DATETIME": open_datetime,
                    "BUY_SELL": "S",  # We'll do short for the first leg
                    "CALL_PUT": trade["OptionType"],
                    "STRIKE": trade["ShortStrike"],
                    "EXPIRATION": trade["OpenDate"],
                    "QUANTITY": trade["qty"],
                }
            )
            signal_data.append(
                {
                    "OPEN_DATETIME": open_datetime,
                    "BUY_SELL": "B",  # We'll do long for the second leg
                    "CALL_PUT": trade["OptionType"],
                    "STRIKE": trade["LongStrike"],
                    "EXPIRATION": trade["OpenDate"],
                    "QUANTITY": trade["qty"],
                }
            )

    path = os.path.dirname(filename)
    basename = os.path.basename(filename)
    result_df = pd.DataFrame(signal_data)
    result_df.to_csv(filename, index=False)  # full signal file with puts and calls
    for right in ["Puts", "Calls"]:  # separate signal files
        filtered = result_df[result_df["CALL_PUT"] == right[0]]
        file_path = os.path.join(path, f"{right}_{basename}")
        filtered.to_csv(file_path, index=False)

    return result_df


@with_gc
def walk_forward_test(
    results_queue: Queue = None,
    cancel_flag=None,
    df_dicts: dict | str = None,
    path: str = None,
    strategy_settings: dict = None,
    start: dt.date = None,
    end: dt.date = None,
    initial_value: float = 100_000,
    use_scaling=False,
    export_trades=False,
    export_OO_sig=False,
    weekday_list: list = [],
    news_events: dict = {},
    optimizer_result: OptimizerResult = None,
):
    global logger
    logger = setup_logging(logger, "DEBUG")
    try:
        logger.info("Starting Walk-Forward test...")
        if isinstance(df_dicts, str):
            logger.debug("Loading df_dicts from pickle file...")
            with open(df_dicts, "rb") as f:
                df_dicts_pkl = pickle.load(f)
            logger.debug("df_dicts loaded from pickle file.")
            logger.debug(f"Removing pickle file: {df_dicts}")
            try:
                os.remove(df_dicts)
            except Exception as e:
                logger.error(f"Failed to remove pickle file: {e}")

            df_dicts = df_dicts_pkl

        portfolio_mode = "-SINGLE_MODE-" not in strategy_settings
        start_date = dt.date.min
        passthrough_start_date = dt.date.min
        end_date = dt.date.max
        # loop through all the source dfs to find start/end dates
        for source, df_dict in df_dicts["Put-Call Comb"]["All"].items():
            try:
                passthrough = strategy_settings[f"{source}.csv"]["-PASSTHROUGH_MODE-"]
            except KeyError as e:
                passthrough = False

            _start_date = df_dict["org_df"]["EntryTime"].min().date()
            _end_date = df_dict["org_df"]["EntryTime"].max().date()
            # find the latest start date
            if not passthrough:
                if _start_date > start_date:
                    start_date = _start_date
            else:
                # we need to treat passthrough separate since there is no
                # warm up period necessary.
                if _start_date > passthrough_start_date:
                    passthrough_start_date = _start_date

            # find the earliest end date passthrough doesn't matter here
            if _end_date < end_date:
                end_date = _end_date
        max_long_avg_period = 0
        for settings in strategy_settings.values():
            max_long_avg_period = max(
                max(settings["-AVG_PERIOD_1-"], settings["-AVG_PERIOD_2-"]),
                max_long_avg_period,
            )
        date_adv = start_date + relativedelta(months=max_long_avg_period)
        warm_start = dt.date(date_adv.year, date_adv.month, 1)
        # use either the user input date or the first warmed up date
        if start:
            start_test_date = start
        else:
            start_test_date = warm_start
        end = end_date if end is None else end

        # check if any strats are using auto exclusion
        warm_up_date = start_test_date
        using_auto_exclusions = False
        for setting in strategy_settings.values():
            if setting["-AUTO_EXCLUSIONS-"]:
                # set the warmup date
                warm_up_date = warm_start + relativedelta(months=max_long_avg_period)
                using_auto_exclusions = True
                break
        # now we just need to see if the passthrough strats start later
        warm_up_date = max(warm_up_date, passthrough_start_date)

        if not portfolio_mode:
            settings = strategy_settings["-SINGLE_MODE-"]
            strats = ["All-P_C_Comb"]
            if settings["-PUT_OR_CALL-"] and settings["-IDV_WEEKDAY-"]:
                strats += ["Weekday-P_C_Comb", "All-Best_P_or_C", "Weekday-Best_P_or_C"]
            elif settings["-IDV_WEEKDAY-"]:
                strats.append("Weekday-P_C_Comb")
            elif settings["-PUT_OR_CALL-"]:
                strats.append("All-Best_P_or_C")
            if settings["-GAP_ANALYSIS-"]:
                for _strat in strats.copy():
                    strats.append(f"{_strat}-Gap")
        else:
            strats = ["Portfolio"] + list(strategy_settings.keys())

        portfolio_metrics = {}
        for _strat in strats:
            portfolio_metrics[_strat] = {
                "Current Value": initial_value,
                "Highest Value": initial_value,
                "Max DD": 0.0,
                "Current DD": 0.0,
                "DD Days": 0,
                "Tranche Qtys": [],
                "Port Tranche Qtys": [],
                "Num Tranches": 1,
                "Port Num Tranches": 1,
                "trade log": pd.DataFrame(),
                "Tlog Auto Exclusions": pd.DataFrame(),  # for warm-up to calc EV for auto exclusions
                "Win Streak": 0,
                "Loss Streak": 0,
            }

        if portfolio_mode:
            port_dict = portfolio_metrics["Portfolio"]

        # init results
        results = {}
        for strategy in portfolio_metrics:
            results[strategy] = pd.DataFrame()

        # convert weekdays from full day name to short name. i.e. Monday to Mon
        day_list = [_day[:3] for _day in weekday_list]

        def determine_auto_skip(
            date: dt.date, tlog: pd.DataFrame, agg_type: str
        ) -> bool:
            """
            Calculate the expected value of any news events that
            occur on the given date and return True if negative expectancy
            """
            current_weekday = date.strftime("%a")
            agg_type = (
                "ME"
                if agg_type == "Monthly"
                else "SME" if agg_type == "Semi-Monthly" else "W-SAT"
            )
            trade_log = tlog.copy()
            trade_log["EntryTime"] = pd.to_datetime(trade_log["EntryTime"])
            if is_BYOB_data(trade_log):
                trade_log["P/L"] = (
                    trade_log["ProfitLossAfterSlippage"] * 100
                    - trade_log["CommissionFees"]
                )

            def _get_current_rolling_avg(df):
                # Set 'EntryTime' as the index
                df = df.set_index("EntryTime")
                # Resample to monthly or weekly frequency, summing the PNL
                aggregated_pnl = df["P/L"].resample(agg_type).sum()
                # Calculate the rolling average
                window = (
                    max_long_avg_period
                    if agg_type == "ME"
                    else (
                        int(max_long_avg_period * 2)
                        if agg_type == "SME"
                        else int(max_long_avg_period * 4.33)
                    )
                )
                rolling_avg_pnl = aggregated_pnl.rolling(
                    window=window, min_periods=1
                ).mean()
                if not rolling_avg_pnl.empty:
                    return rolling_avg_pnl.iloc[-1]
                else:
                    return 0

            # find the events that occur on this date and calc the expectancy
            for event, date_list in news_events.items():
                if date in date_list:
                    trade_log_filtered = trade_log[
                        trade_log["EntryTime"].dt.date.isin(date_list)
                    ]
                    if not trade_log_filtered.empty:
                        current_avg = _get_current_rolling_avg(trade_log_filtered)
                        if current_avg < 0:
                            # this event has negative expectancy, whole day can be skipped
                            return True

            # passed all news events, lets see if we skip the weekday
            trade_log_filtered = trade_log[
                trade_log["Day of Week"].str.contains(current_weekday)
            ]
            if not trade_log_filtered.empty:
                current_avg = _get_current_rolling_avg(trade_log_filtered)
                if current_avg < 0:
                    # this dat has negative expectancy, whole day can be skipped
                    return True
            return False

        if using_auto_exclusions:
            current_date = warm_start
        else:
            current_date = max(start_test_date, passthrough_start_date)

        # determine if we need to use gaps
        spx_history = pd.DataFrame()
        for setting in strategy_settings.values():
            if setting["-GAP_ANALYSIS-"]:
                spx_history = get_spx_gaps(current_date, end)
                if not spx_history.empty:
                    # reset the index to just the date, dropping the time component
                    spx_history = spx_history.reset_index()
                    spx_history["Date"] = spx_history["Date"].dt.date
                    spx_history = spx_history.set_index("Date")

        while current_date <= end:
            if not optimizer_result:
                # only log progress if we are not doing an optimization
                logger.debug(f"WF Test - Current date: {current_date}")

            # check for cancel flag to stop thread
            if cancel_flag is not None and cancel_flag.is_set():
                cancel_flag.clear()
                results_queue.put(("-BACKTEST_CANCELED-", "-WALK_FORWARD_TEST-"))
                return

            warmed_up = current_date >= warm_up_date

            if portfolio_mode:
                # reset daily pnl for portfolio
                port_dict["Current Day PnL"] = 0

            current_weekday = current_date.strftime("%a")
            for strat, strat_dict in portfolio_metrics.items():
                if portfolio_mode and strat == "Portfolio":
                    # we don't trade the portfolio, it is just the combination of all individual strats
                    continue
                elif portfolio_mode:
                    settings = strategy_settings[strat]
                else:
                    settings = strategy_settings["-SINGLE_MODE-"]

                # reset daily pnl for individual strategy
                strat_dict["Current Day PnL"] = 0

                day_exclusions = []
                news_date_exclusions = []
                if settings["-APPLY_EXCLUSIONS-"] != "Analysis":
                    # we are applying exclusions to either the WF test or both the WF and Analysis
                    day_exclusions = [
                        _day[:3] for _day in settings["-WEEKDAY_EXCLUSIONS-"]
                    ]
                    # get list of news event dates to skip.
                    for release, date_list in news_events.items():
                        if release in settings["-NEWS_EXCLUSIONS-"]:
                            news_date_exclusions += date_list

                skip_day = False
                if warmed_up and using_auto_exclusions:
                    skip_day = determine_auto_skip(
                        current_date,
                        strat_dict["Tlog Auto Exclusions"],
                        settings["-AGG_TYPE-"],
                    )
                elif (
                    current_weekday in day_exclusions
                    or current_weekday not in day_list
                    or current_date in news_date_exclusions
                ):
                    skip_day = True

                if not settings["-PASSTHROUGH_MODE-"]:
                    if use_scaling:

                        def determine_num_tranches(
                            min_tranches, max_tranches, num_contracts
                        ):
                            tranches = max_tranches
                            while True:
                                if num_contracts > tranches:
                                    max_tranche_qty = int(num_contracts / tranches)
                                    remain_qty = num_contracts - (
                                        tranches * max_tranche_qty
                                    )
                                    if remain_qty >= min_tranches or remain_qty == 0:
                                        # we're done we can stay at this number of tranches with
                                        # the remainder filling up another set of at least min tranches
                                        return tranches
                                    else:
                                        # we need to take a tranche away so we can try to fill up at
                                        # least 1 full set at min amount
                                        if tranches - 1 < min_tranches:
                                            # we can't reduce any further, got with what we have
                                            # even if that means we will be adding contracts below the min
                                            return tranches
                                        else:
                                            tranches -= 1
                                else:
                                    return num_contracts

                        def determine_tranche_qtys(tranches):
                            tranche_qtys = []
                            for x in range(tranches):
                                if x < num_contracts % tranches:
                                    # this is where we add the remaining contracts after filling up all tranches
                                    tranche_qtys.append(
                                        int(num_contracts / tranches) + 1
                                    )
                                else:
                                    tranche_qtys.append(int(num_contracts / tranches))
                            return tranche_qtys

                        min_tranches = settings["-MIN_TRANCHES-"]
                        max_tranches = settings["-MAX_TRANCHES-"]
                        bp_per_contract = settings["-BP_PER-"]
                        num_contracts = int(
                            strat_dict["Current Value"] / bp_per_contract
                        )
                        tranches = determine_num_tranches(
                            min_tranches, max_tranches, num_contracts
                        )
                        strat_dict["Num Tranches"] = tranches
                        strat_dict["Tranche Qtys"] = determine_tranche_qtys(tranches)
                        if portfolio_mode:
                            weighted_value = (
                                port_dict["Current Value"]
                                * settings["-PORT_WEIGHT-"]
                                / 100
                            )
                            num_contracts = int(weighted_value / bp_per_contract)
                            tranches = determine_num_tranches(
                                min_tranches, max_tranches, num_contracts
                            )
                            strat_dict["Port Num Tranches"] = tranches
                            strat_dict["Port Tranche Qtys"] = determine_tranche_qtys(
                                tranches
                            )
                    else:
                        # not scaling
                        num_contracts = settings["-TOP_X-"]
                        strat_dict["Num Tranches"] = num_contracts
                        strat_dict["Tranche Qtys"] = [1 for x in range(num_contracts)]
                        strat_dict["Port Num Tranches"] = num_contracts
                        strat_dict["Port Tranche Qtys"] = [
                            1 for x in range(num_contracts)
                        ]

                if settings["-AGG_TYPE-"] == "Monthly":
                    # date for best times should be the month prior as we don't know the future yet
                    best_time_date = current_date - relativedelta(months=1)
                elif settings["-AGG_TYPE-"] == "Semi-Monthly":
                    # grab from last half-month
                    best_time_date = current_date - relativedelta(days=15)
                else:
                    # grab from last week
                    best_time_date = current_date - relativedelta(weeks=1)

                def log_pnl_and_trades(strat_dict, num_tranches, tranche_qtys):
                    # determine gap info
                    gap_str = ""
                    if settings["-GAP_ANALYSIS-"]:
                        _gap_type = "Gap%" if settings["-GAP_TYPE-"] == "%" else "Gap"
                        try:
                            gap_value = spx_history.at[current_date, _gap_type]
                        except KeyError as e:
                            # probably a day market was not open (i.e. holiday)
                            gap_value = 0
                        if gap_value > settings["-GAP_THRESHOLD-"]:
                            gap_str = " Gap Up"
                        elif gap_value < -settings["-GAP_THRESHOLD-"]:
                            gap_str = " Gap Down"

                    if portfolio_mode:
                        # determine which strat to use
                        if settings["-PUT_OR_CALL-"]:
                            _strat = "Best P/C"
                        else:
                            _strat = "Put-Call Comb"

                        _strat = (
                            _strat + gap_str
                        )  # add gap info onto the end of strat name

                        # determine which weekday to use
                        if settings["-IDV_WEEKDAY-"]:
                            _weekday = current_weekday
                        else:
                            _weekday = "All"

                    else:
                        # determine strat name for df_dicts
                        if "P_C_Comb" in strat:
                            _strat = "Put-Call Comb"
                        else:
                            _strat = "Best P/C"

                        # determine gap type
                        if "Gap" not in strat:
                            gap_str = ""

                        _strat = _strat + gap_str  # add gap onto the end of strat name

                        # determine weekday type
                        if strat.startswith("All"):
                            _weekday = "All"
                        else:
                            _weekday = current_weekday

                    # finally select the appropriate df_dict and get times
                    df_dict = df_dicts[_strat][_weekday]

                    if portfolio_mode:
                        source = os.path.splitext(strat)[0]
                        if settings["-PASSTHROUGH_MODE-"]:
                            # get all the times this traded on this date
                            source_df = df_dicts["Put-Call Comb"]["All"][source][
                                "org_df"
                            ]
                            _filtered_df = source_df[
                                source_df["EntryTime"].dt.date == current_date
                            ]
                            best_times = (
                                _filtered_df["EntryTime"]
                                .dt.strftime("%H:%M:%S")
                                .unique()
                                .tolist()
                            )

                            # Let's determine the qtys to trade for each trade in the log.
                            tranche_qtys = []
                            for _ in best_times:
                                if use_scaling:
                                    current_value = strat_dict["Current Value"]
                                    # calc total qty
                                    total_qty = (
                                        current_value
                                        * settings["-PORT_WEIGHT-"]
                                        / 100
                                        / settings["-BP_PER-"]
                                    )

                                    # qty per trade
                                    qty = int(total_qty / len(best_times))
                                    tranche_qtys.append(max(qty, 1))
                                else:
                                    tranche_qtys.append(1)
                        else:
                            best_times_df = get_top_times(
                                df_dict, strategy_settings, best_time_date, num_tranches
                            )
                            # filter out other sources since all sources are included
                            best_times_df = (
                                best_times_df[
                                    best_times_df["Source"].str.endswith(source)
                                ]
                                .sort_values("Values", ascending=False)
                                .head(num_tranches)
                            )
                            best_times = best_times_df["Top Times"].to_list()
                    else:
                        best_times_df = get_top_times(
                            df_dict, strategy_settings, best_time_date, num_tranches
                        )
                        best_times = best_times_df["Top Times"].to_list()

                    for i, time in enumerate(best_times):
                        # get the qty for this tranche time
                        if tranche_qtys:
                            qty = tranche_qtys[i]
                        else:
                            # we probably ran out of money
                            qty = 0
                        full_dt = dt.datetime.combine(
                            current_date, dt.datetime.strptime(time, "%H:%M:%S").time()
                        )

                        if not settings["-PASSTHROUGH_MODE-"]:
                            # get the source df, we already have it from earlier for pass-through
                            source = best_times_df.loc[
                                best_times_df["Top Times"] == time, "Source"
                            ].values[0]
                            source_df = df_dict[source]["org_df"]

                        filtered_rows = source_df[
                            source_df["EntryTime"] == full_dt
                        ].copy()

                        if filtered_rows.empty:
                            continue

                        filtered_rows["qty"] = qty
                        filtered_rows["source"] = source

                        if is_BYOB_data(source_df):
                            gross_pnl = (
                                filtered_rows["ProfitLossAfterSlippage"].sum()
                                * 100
                                * qty
                            )
                            commissions = filtered_rows["CommissionFees"].sum() * qty
                            pnl = gross_pnl - commissions
                        else:
                            pnl = filtered_rows["P/L"].sum() * qty

                        # log trade
                        strat_dict["Tlog Auto Exclusions"] = pd.concat(
                            [strat_dict["Tlog Auto Exclusions"], filtered_rows],
                            ignore_index=True,
                        )
                        if warmed_up and not skip_day:
                            strat_dict["trade log"] = pd.concat(
                                [strat_dict["trade log"], filtered_rows],
                                ignore_index=True,
                            )
                            strat_dict["Current Value"] += pnl
                            strat_dict["Current Day PnL"] += pnl

                if current_weekday in day_list:
                    # make sure its not the weekend
                    num_tranches = strat_dict["Num Tranches"]
                    tranche_qtys = strat_dict["Tranche Qtys"]
                    log_pnl_and_trades(strat_dict, num_tranches, tranche_qtys)
                    if portfolio_mode:
                        num_tranches = strat_dict["Port Num Tranches"]
                        tranche_qtys = strat_dict["Port Tranche Qtys"]
                        log_pnl_and_trades(port_dict, num_tranches, tranche_qtys)

                def calc_metrics(strat_dict: dict, strat: str, results: dict) -> None:
                    # calc metrics and log the results for the day
                    if strat_dict["Current Value"] >= strat_dict["Highest Value"]:
                        strat_dict["Highest Value"] = strat_dict["Current Value"]
                        strat_dict["DD Days"] = 0
                    else:
                        # we are in Drawdown
                        dd = (
                            strat_dict["Highest Value"] - strat_dict["Current Value"]
                        ) / strat_dict["Highest Value"]
                        strat_dict["Current DD"] = dd
                        if dd > strat_dict["Max DD"]:
                            strat_dict["Max DD"] = dd
                        strat_dict["DD Days"] += 1

                    if strat_dict["Current Day PnL"] > 0:
                        strat_dict["Win Streak"] += 1
                        strat_dict["Loss Streak"] = 0
                    elif strat_dict["Current Day PnL"] < 0:
                        # tie does not change any streak
                        strat_dict["Win Streak"] = 0
                        strat_dict["Loss Streak"] += 1

                    new_row = pd.DataFrame(
                        [
                            {
                                "Date": current_date,
                                "Current Value": strat_dict["Current Value"],
                                "Highest Value": strat_dict["Highest Value"],
                                "Max DD": strat_dict["Max DD"],
                                "Current DD": strat_dict["Current DD"],
                                "DD Days": strat_dict["DD Days"],
                                "Day PnL": strat_dict["Current Day PnL"],
                                "Win Streak": strat_dict["Win Streak"],
                                "Loss Streak": strat_dict["Loss Streak"],
                                "Initial Value": initial_value,
                                "Weekday": current_weekday,
                            }
                        ]
                    )
                    results[strat] = pd.concat(
                        [results[strat], new_row], ignore_index=True
                    )

                if warmed_up and not skip_day:
                    calc_metrics(strat_dict, strat, results)

                if skip_day:
                    # this is a skip day just increment the DD days if needed
                    if strat_dict["DD Days"] > 0:
                        strat_dict["DD Days"] += 1
                    if portfolio_mode and port_dict["DD Days"] > 0:
                        port_dict["DD Days"] += 1

            # calculate all the stats for the portfolio now that all other strats have traded
            if portfolio_mode and warmed_up and not skip_day:
                calc_metrics(port_dict, "Portfolio", results)

            current_date += dt.timedelta(1)

        for strat in portfolio_metrics:
            if not results[strat].empty:
                results[strat]["Date"] = pd.to_datetime(results[strat]["Date"])
                uuid_str = str(uuid.uuid4())[:8]
                if export_trades:
                    base_filename = f"{strat} - TradeLog_{uuid_str}"
                    ext = ".csv"
                    export_filename = get_next_filename(path, base_filename, ext)
                    portfolio_metrics[strat]["trade log"].to_csv(
                        export_filename, index=False
                    )
                if export_OO_sig:
                    base_filename = f"{strat} - OO_Signal_File_{uuid_str}"
                    ext = ".csv"
                    data = portfolio_metrics[strat]["trade log"]
                    export_filename = get_next_filename(path, base_filename, ext)
                    # Export the overall Signal File for the Strat
                    export_oo_sig_file(data, export_filename)

                    # Export the signals for each csv (strategy) separately
                    if not portfolio_mode:
                        strategies = data["Strategy"].unique()
                        for strategy in strategies:
                            base_filename = (
                                f"{strat} - OO_Signal_File_{strategy}_{uuid_str}"
                            )
                            ext = ".csv"
                            export_filename = get_next_filename(
                                path, base_filename, ext
                            )
                            export_oo_sig_file(
                                data[data["Strategy"] == strategy], export_filename
                            )
        if results_queue:
            results_queue.put(("-BACKTEST_END-", results))
        if optimizer_result is not None:
            optimizer_result.wf_result = results
            return optimizer_result
        return results
    except Exception as e:
        if results_queue:
            results_queue.put(("-BACKTEST_END-", e))
        logger.exception("Exception in walk_forward_test")
