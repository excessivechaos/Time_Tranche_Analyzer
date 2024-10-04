from loguru import logger
import random
import numpy as np
import os
import datetime as dt
from tta_analysis import run_analysis_threaded
from tta_wf_test import walk_forward_test
from tta_helpers import with_gc, setup_logging, load_data
from multiprocessing import Pool, Event, Queue
import time
from dateutil.relativedelta import relativedelta
import copy
from optimizer_result_model import OptimizerResult
import uuid


def calc_metrics(optimizer_result: OptimizerResult) -> OptimizerResult:
    """Calc metrics for a single strategy"""
    results = optimizer_result.wf_result
    df = list(results.values())[0]
    # Calculate summary statistics for the strategy
    final_value = df["Current Value"].iloc[-1]
    max_dd = df["Max DD"].max()
    setattr(optimizer_result, "Drawdown%", max_dd)
    dd_days = df["DD Days"].max()
    setattr(optimizer_result, "Days in Drawdown", dd_days)
    initial_value = df["Initial Value"].min()
    total_return = (final_value - initial_value) / initial_value
    setattr(optimizer_result, "Total Return", total_return)
    # CAGR
    start_dt = df["Date"].iloc[0]
    end_dt = df["Date"].iloc[-1]
    years = (end_dt - start_dt).days / 365.25
    cagr = ((final_value / initial_value) ** (1 / years)) - 1
    setattr(optimizer_result, "CAGR", cagr)

    # Sharpe Ratio
    df["Daily Return"] = df["Current Value"].pct_change()
    std_dev = df["Daily Return"].std()
    risk_free_rate = 0.02 / 252  # Assume 2% annual risk-free rate, convert to daily
    excess_returns = df["Daily Return"] - risk_free_rate
    sharpe_ratio = np.sqrt(252) * excess_returns.mean() / std_dev  # Annualized
    setattr(optimizer_result, "Sharpe", sharpe_ratio)
    if max_dd:
        mar = cagr / max_dd
    else:
        mar = float("inf")
    setattr(optimizer_result, "MAR", mar)

    # Group PnL by month
    df["YearMonth"] = df["Date"].dt.to_period("M")
    monthly_pnl = df.groupby("YearMonth")["Day PnL"].sum()

    # Calculate largest and lowest monthly PnL with their corresponding dates
    largest_monthly_pnl = monthly_pnl.max()
    setattr(optimizer_result, "Largest Month", largest_monthly_pnl)
    lowest_monthly_pnl = monthly_pnl.min()
    setattr(optimizer_result, "Smallest Month", lowest_monthly_pnl)
    return optimizer_result


def run_analysis_wrapper(kwargs):
    return run_analysis_threaded(**kwargs)


def wf_test_wrapper(kwargs):
    return walk_forward_test(**kwargs)


@with_gc
def optimizer(
    file,
    generations: int = 3,
    children: int = 5,
    selection_metric: str = "MAR",
    num_parents: int = 10,
    cancel_flag: Event = None,  # type: ignore
    results_queue: Queue = None,
    weekday_list: list = [],
    news_events: dict = {},
):
    setup_logging("DEBUG")
    try:
        reverse_sort = selection_metric in [
            "MAR",
            "Sharpe",
            "CAGR",
            "Total Return",
            "Largest Month",
            "Smallest Month",
        ]

        def get_strat_settings_random(
            pre_select: dict = None, bp_per: float = 6000
        ) -> dict:
            settings = {}
            if pre_select:  #
                settings.update(pre_select)
            if "-AVG_PERIOD_2-" not in settings and "-AVG_PERIOD_1-" not in settings:
                settings["-AVG_PERIOD_2-"] = random.choice([x for x in range(1, 13)])
            elif "-AVG_PERIOD_2-" not in settings and "-AVG_PERIOD_1-" in settings:
                # pick a period at random that is greater than the avg period 1, or 12 if period 1 is 12
                settings["-AVG_PERIOD_2-"] = random.choice(
                    min([x for x in range(settings["-AVG_PERIOD_1-"], 13)], [12])
                )
            if "-AVG_PERIOD_1-" not in settings:
                settings["-AVG_PERIOD_1-"] = random.choice(
                    max([x for x in range(1, settings["-AVG_PERIOD_2-"])], [1])
                )
            if (
                "-PERIOD_1_WEIGHT-" not in settings
                and "-PERIOD_2_WEIGHT-" not in settings
            ):
                settings["-PERIOD_1_WEIGHT-"] = random.choice(
                    [x for x in range(5, 101, 5)]
                )
                settings["-PERIOD_2_WEIGHT-"] = 100 - settings["-PERIOD_1_WEIGHT-"]
            elif (
                "-PERIOD_1_WEIGHT-" in settings and "-PERIOD_2_WEIGHT-" not in settings
            ):
                settings["-PERIOD_2_WEIGHT-"] = 100 - settings["-PERIOD_1_WEIGHT-"]
            elif (
                "-PERIOD_2_WEIGHT-" in settings and "-PERIOD_1_WEIGHT-" not in settings
            ):
                settings["-PERIOD_1_WEIGHT-"] = 100 - settings["-PERIOD_2_WEIGHT-"]
            if "-TOP_X-" not in settings:
                settings["-TOP_X-"] = random.choice([x for x in range(1, 16)])
            if "-CALC_TYPE-" not in settings:
                settings["-CALC_TYPE-"] = random.choice(["PCR", "PnL"])
            if "-AGG_TYPE-" not in settings:
                settings["-AGG_TYPE-"] = random.choice(
                    ["Monthly", "Semi-Monthly", "Weekly"]
                )
            if "-PUT_OR_CALL-" not in settings:
                settings["-PUT_OR_CALL-"] = random.choice([True, False])
            if "-IDV_WEEKDAY-" not in settings:
                settings["-IDV_WEEKDAY-"] = random.choice([True, False])
            # Non-random settings
            settings["-MIN_TRANCHES-"] = settings["-TOP_X-"]
            settings["-MAX_TRANCHES-"] = settings["-TOP_X-"]
            settings["-BP_PER-"] = bp_per
            settings["-PASSTHROUGH_MODE-"] = False
            settings["-PORT_WEIGHT-"] = 100
            settings["-TOP_TIME_THRESHOLD-"] = float("-inf")
            settings["-APPLY_EXCLUSIONS-"] = "Both"
            settings["-GAP_THRESHOLD-"] = 0
            settings["-GAP_TYPE-"] = "%"
            # Initialize option settings if they don't exist
            for option in [
                "-WEEKDAY_EXCLUSIONS-",
                "-NEWS_EXCLUSIONS-",
                "-AUTO_EXCLUSIONS-",
                "-GAP_ANALYSIS-",
            ]:
                if option not in settings:
                    settings[option] = []
            return settings

        try:
            # we will use all but 1 cpu, so hopefully the host
            # other programs will not slow too much.
            cpu_count = max(os.cpu_count() - 1, 1)
        except Exception as e:
            logger.exception("Error retrieving CPU count")
            cpu_count = 2

        # load the df to get the start date
        _df, start_date, _end_date = load_data(file)
        # set the start date for the WF test to 12mo later so we
        # have a normalized and warmed up start point for all tests
        start_date = start_date + relativedelta(months=12)
        total_tests = 0
        test_results = {}
        start_time = time.time()
        with Pool(processes=cpu_count) as pool:

            def run_genetic_test(run_analysis_kwargs_list, start_date: dt.date = None):
                nonlocal total_tests
                setup_logging("DEBUG")
                analysis_results = pool.map(
                    run_analysis_wrapper, run_analysis_kwargs_list
                )
                for result in analysis_results:
                    if isinstance(result, Exception):
                        raise result
                logger.debug(f"analysis results: {len(analysis_results)}")
                wf_test_kwargs_list = []
                for result in analysis_results:
                    wf_test_kwargs = {
                        "results_queue": None,
                        "cancel_flag": None,
                        "df_dicts": result.run_analysis_result,
                        "path": "",
                        "strategy_settings": {result.strat_name: result.settings},
                        "use_scaling": True,
                        "start": start_date,
                        "weekday_list": weekday_list,
                        "news_events": news_events,
                        "optimizer_result": result,
                    }
                    wf_test_kwargs_list.append(wf_test_kwargs)
                # Check for cancel flag
                if cancel_flag is not None and cancel_flag.is_set():
                    return
                wf_test_results = pool.map(wf_test_wrapper, wf_test_kwargs_list)
                for result in wf_test_results:
                    if isinstance(result, Exception):
                        raise result
                logger.debug(f"wf test results: {len(wf_test_results)}")
                total_tests += len(wf_test_results)

                # Check for cancel flag
                if cancel_flag is not None and cancel_flag.is_set():
                    return
                calc_metric_results = pool.map(calc_metrics, wf_test_results)

                return calc_metric_results

            settings_history = []
            strat_name = os.path.basename(file)
            run_analysis_kwargs_list = []
            for _ in range(num_parents):  # create n random parents
                settings = {strat_name: get_strat_settings_random()}
                counter = 0
                while settings in settings_history:
                    if counter > 99:
                        # we seem to be having trouble finding a unique
                        # configuration, we should just move on with what
                        # we have.
                        logger.debug(
                            "Unable to find a unique config for initial parent, moving on"
                        )
                        break
                    # this configuration is already in or been tested
                    # get new settings
                    settings = {strat_name: get_strat_settings_random()}
                    logger.debug(
                        "We selected a configuration of settings that has already been selected for the initial test, getting a new random configuration"
                    )
                    counter += 1
                # add the settings to history
                settings_history.append(settings)
                new_parent = OptimizerResult(
                    strat_name,
                    settings[strat_name],
                    selection_metric=selection_metric,
                    lineage=uuid.uuid4(),
                )
                run_analysis_threaded_kwargs = {
                    "files_list": [file],
                    "results_queue": None,
                    "cancel_flag": None,
                    "strategy_settings": settings,
                    "open_files": False,
                    "create_excel": False,
                    "news_events": news_events,
                    "optimizer_result": new_parent,
                }
                new_parent.run_analysis_kwargs = run_analysis_threaded_kwargs
                run_analysis_kwargs_list.append(run_analysis_threaded_kwargs)

            logger.debug(
                f"Starting optimization with {num_parents} parents, {children} children, {generations} generations"
            )

            # Check for cancel flag before running tests
            if cancel_flag is not None and cancel_flag.is_set():
                cancel_flag.clear()
                if results_queue:
                    results_queue.put(("-BACKTEST_CANCELED-", "-OPTIMIZER-"))
                return
            results = run_genetic_test(
                run_analysis_kwargs_list, start_date=start_date
            )  # run the initial test
            # Check for cancel flag again in case it happened during
            if cancel_flag is not None and cancel_flag.is_set():
                cancel_flag.clear()
                if results_queue:
                    results_queue.put(("-BACKTEST_CANCELED-", "-OPTIMIZER-"))
                return

            for result in results:
                test_results[result.lineage] = [result]  # add the original parent

            msg = "\nResults for initial parents:"
            for i, result_list in enumerate(test_results.values()):
                msg += f"\n{i+1}: {result_list[0]}"
            logger.debug(msg)
            logger.debug("----------------------------------------------------\n")

            key_traits = [
                "-AVG_PERIOD_1-",
                "-PERIOD_1_WEIGHT-",
                "-AVG_PERIOD_2-",
                "-TOP_X-",
                "-CALC_TYPE-",
                "-AGG_TYPE-",
                "-PUT_OR_CALL-",
                "-IDV_WEEKDAY-",
            ]
            num_mutations = 1
            # run the genetic algorithm for the specified number of generations
            for generation in range(generations):
                run_analysis_kwargs_list = []
                # we will spawn children for each of the parents
                for parent_lineage, parent_list in test_results.items():
                    # get the best result from the parent lineage
                    parent: OptimizerResult = sorted(
                        parent_list,
                        key=lambda x: getattr(x, x.selection_metric),
                        reverse=reverse_sort,
                    )[0]
                    # create children that inherit all but up to n traits that will be mutated
                    for _child in range(children):
                        # how many traits to mutate
                        mutated_traits = random.sample(
                            key_traits, k=random.randint(1, num_mutations)
                        )
                        # copy the parent settings
                        pre_select = copy.deepcopy(parent.settings)

                        for trait in mutated_traits:
                            # remove the trait that we will mutate on
                            del pre_select[trait]
                        # get new mutated traits
                        settings = {
                            strat_name: get_strat_settings_random(pre_select=pre_select)
                        }
                        # we need to make sure we haven't already used this configuration
                        # if we have we will remove an inherited trait.
                        available_traits = [
                            x for x in key_traits if x not in mutated_traits
                        ]
                        counter = 0
                        while settings in settings_history:
                            if counter > 99:
                                if available_traits:
                                    # we can't seem to find a new genetically diverse
                                    # child to test.  Perhaps all possible combinations
                                    # have been tested with these mutated traits.
                                    # lets remove an inherited trait and try again
                                    logger.debug(
                                        "Couldn't find different configuration, mutating an additional trait"
                                    )
                                    removed_trait = random.choice(available_traits)
                                    available_traits.remove(removed_trait)
                                    del pre_select[removed_trait]
                                    counter = 0
                                else:
                                    # we removed all inherited traits and still cannot
                                    # find a genetically different child.  We may have
                                    # exhausted all possible combinations, so lets just
                                    # continue with what we have so the test can finish
                                    logger.debug(
                                        f"Could not find a new genetically different configuration from what has already been tested"
                                    )
                                    break
                            # this configuration is already selected or has been tested
                            # get new settings
                            settings = {
                                strat_name: get_strat_settings_random(
                                    pre_select=pre_select
                                )
                            }
                            counter += 1
                        # add the settings to history
                        settings_history.append(settings)
                        # new optimizer result object to hold the results of the test
                        new_parent = OptimizerResult(
                            strat_name,
                            settings[strat_name],
                            selection_metric=selection_metric,
                            lineage=parent_lineage,
                        )
                        # build the kwargs to run the analysis
                        run_analysis_threaded_kwargs = {
                            "files_list": [file],
                            "results_queue": None,
                            "cancel_flag": None,
                            "strategy_settings": settings,
                            "open_files": False,
                            "create_excel": False,
                            "news_events": news_events,
                            "optimizer_result": new_parent,
                        }
                        new_parent.run_analysis_kwargs = run_analysis_threaded_kwargs
                        # add to list
                        run_analysis_kwargs_list.append(run_analysis_threaded_kwargs)
                # Check for cancel flag before running tests
                if cancel_flag is not None and cancel_flag.is_set():
                    cancel_flag.clear()
                    if results_queue:
                        results_queue.put(("-BACKTEST_CANCELED-", "-OPTIMIZER-"))
                    return
                results = run_genetic_test(
                    run_analysis_kwargs_list,
                    start_date=start_date,
                )
                # Check for cancel flag again in case it happened during
                if cancel_flag is not None and cancel_flag.is_set():
                    cancel_flag.clear()
                    if results_queue:
                        results_queue.put(("-BACKTEST_CANCELED-", "-OPTIMIZER-"))
                    return

                for result in results:
                    test_results[result.lineage].append(result)

                msg = f"\nBest performers for generation {generation+1}:"
                for i, result_list in enumerate(test_results.values()):
                    best_performer = sorted(
                        result_list,
                        key=lambda x: getattr(x, x.selection_metric),
                        reverse=reverse_sort,
                    )[0]
                    msg += f"\n{i+1}: {best_performer}"
                logger.debug(msg)
                logger.debug("----------------------------------------------------\n")

        total_time = time.time() - start_time
        time_per_test = total_time / total_tests
        logger.debug(
            f"Total tests run: {total_tests} - Total Run Time: {total_time} - Time per test: {time_per_test:.3f}"
        )
        best_performers = []
        for result_list in test_results.values():
            best_performer = sorted(
                result_list,
                key=lambda x: getattr(x, x.selection_metric),
                reverse=reverse_sort,
            )[0]
            best_performers.append(best_performer)
        best_performer = sorted(
            best_performers,
            key=lambda x: getattr(x, x.selection_metric),
            reverse=reverse_sort,
        )[0]

        results_queue.put(("-OPTIMIZER-", (best_performer, time_per_test, start_date)))
        return best_performer, time_per_test, start_date
    except Exception as e:
        results_queue.put(("-OPTIMIZER-", e))
        logger.exception("Error in optimizer")
