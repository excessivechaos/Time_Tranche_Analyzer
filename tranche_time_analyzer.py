import ctypes
import datetime as dt
import os
import json
from loguru import logger
import threading
from multiprocessing import Process, Queue, Event, freeze_support, Pool
import webbrowser
import matplotlib
import pandas as pd
import PySimpleGUI as sg
from dateutil import parser
from CSV_merger import main as csv_merger_window
import textwrap
from tta_helpers import (
    Checkbox,
    with_gc,
    donate_paypal_logo,
    donate_venmo_logo,
    icon,
    setup_logging,
    chunk_list,
    get_dpi_scale,
    format_float,
    get_top_times,
    resize_base64_image,
)
from tta_analysis import run_analysis_threaded
from tta_charts import (
    get_correlation_matrix,
    get_monthly_pnl_chart,
    get_news_event_pnl_chart,
    get_pnl_plot,
    get_weekday_pnl_chart,
)
from tta_wf_test import walk_forward_test
from tta_optimizer import genetic_optimizer, exhaustive_optimizer
from optimizer_result_model import OptimizerResult

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

# make app dpi aware
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

__version__ = "v.1.15.3a"
__program_name__ = "Tranche Time Analyzer"


sg.theme("Reddit")
themes = {"Light": "Reddit", "Dark1": "Dark", "Dark2": "DarkGrey11", "Dark3": "Black"}
# themes = {theme:theme for theme in sg.theme_list()}
button_color = sg.theme_button_color()  # get button color from reddit theme
if sg.running_windows():
    font = ("Segoe UI", 10)
else:
    font = ("Arial", 14)
sg.SetOptions(font=font, icon=icon, element_padding=(5, 5))
screen_size = sg.Window.get_screen_size()
image_aspect_ratio = 0.5


# results queue for threads
results_queue = Queue()
cancel_flag = Event()

weekday_list = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
news_events = {
    "CPI": [],
    "Initial Jobless Claims": [],
    "Retail Sales": [],
    "ADP": [],
    "JOLT": [],
    "Unemployment/NFP": [],
    "PPI": [],
    "GDP": [],
    "PCE": [],
    "Triple Witching": [],
    "Beige Book": [],
    "ISM Manufacturing PMI": [],
    "ISM Services PMI": [],
    "S&P Global PMI": [],
    "Fed Chair Speech": [],
    "FOMC Minutes": [],
    "FOMC": [],
    "MI Consumer Sent.": [],
    "Chicago PMI": [],
}
news_events_loaded = False


def import_news_events(filename) -> bool:
    global news_events
    """
    Import CSV downloaded from https://www.fxstreet.com/economic-calendar
    populates the dates for the releases in 'news_events' dict
    """

    def get_triple_witching_dates(
        start_year: int = 2000, end_year: int = dt.datetime.now().year
    ):
        """
        These are not in the calendar and must be calculated
        Triple witching occurs on the third friday of March, June, Sept, Dec
        """
        triple_witching_dates = []

        for year in range(start_year, end_year + 1):
            for month in [3, 6, 9, 12]:  # March, June, September, December
                # Get the first day of the month
                first_day = dt.datetime(year, month, 1)

                # Find the first Friday
                friday = first_day + dt.timedelta(
                    days=(4 - first_day.weekday() + 7) % 7
                )

                # Get the third Friday
                third_friday = friday + dt.timedelta(weeks=2)

                triple_witching_dates.append(third_friday.date())

        return triple_witching_dates

    def get_event(name):
        """
        Helper function to add news_event column to the df
        """
        keyword_dict = {
            "Consumer Price Index": "CPI",
            "Nonfarm Payrolls": "NFP",
            "ADP Employment": "ADP",
            "Initial Jobless Claims": "Initial Jobless Claims",
            "Retail Sales": "Retail Sales",
            "JOLT": "JOLT",
            "Unemployment": "Unemployment/NFP",
            "Producer Price Index": "PPI",
            "Gross Domestic Product": "GDP",
            "Personal Consumption Expenditures": "PCE",
            "Beige Book": "Beige Book",
            "ISM Manufacturing PMI": "ISM Manufacturing PMI",
            "ISM Services PMI": "ISM Services PMI",
            "Fed's Chair": "Fed Chair Speech",
            "FOMC Minutes": "FOMC Minutes",
            "Fed Interest Rate Decision": "FOMC",
            "Michigan Consumer Sentiment Index": "MI Consumer Sent.",
            "Chicago Purchasing": "Chicago PMI",
        }

        if "S&P" in name and "PMI" in name:
            return "S&P Global PMI"
        else:
            for keyword, event in keyword_dict.items():
                if keyword in name:
                    return event
            return ""

    # load csv, config dates and filter for US events
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        return
    if (
        "Start" not in df.columns
        or "Currency" not in df.columns
        or "Name" not in df.columns
    ):
        return
    df.drop_duplicates(inplace=True)
    df["Start"] = pd.to_datetime(df["Start"])
    df = df[df["Currency"] == "USD"]
    df["news_event"] = df["Name"].apply(get_event)

    for news_event in news_events:
        if news_event == "Triple Witching":
            news_events[news_event] = get_triple_witching_dates()
        else:
            filtered_df = df[df["news_event"] == news_event]
            news_events[news_event] = sorted(filtered_df["Start"].dt.date.to_list())

    return news_events


def find_and_import_news_events(results_queue: Queue):
    best_file = None
    max_rows = 0
    required_columns = set(["Id", "Start", "Name", "Impact", "Currency"])

    # Loop through all files in the current directory
    for filename in os.listdir("."):
        if filename.endswith(".csv"):
            try:
                # Try to read the CSV file
                df = pd.read_csv(filename)

                # Check if the required columns are present
                if set(df.columns) == required_columns:
                    rows = len(df)

                    # If this is the first valid file or has more rows than the previous best
                    if best_file is None or rows > max_rows:
                        best_file = filename
                        max_rows = rows
            except Exception as e:
                # If there's an error reading the file, skip it
                continue

    # If a valid file was found, import the news events
    if best_file:
        result = import_news_events(best_file)
        if result:
            results_queue.put(("-IMPORT_NEWS-", result))
            results_queue.put(
                ("-IMPORT_NEWS-", "News event list found and loaded successfully!")
            )
            return
    results_queue.put(
        (
            "-IMPORT_NEWS-",
            "Could not locate news event csv.\nPlease select under options if needed",
        )
    )


def save_settings(settings, settings_filename, values):
    for key in settings:
        if key in values:
            settings[key] = values[key]
    os.makedirs(os.path.dirname(settings_filename), exist_ok=True)
    with open(settings_filename, "w") as f:
        json.dump(settings, f, indent=4)


def set_default_app_settings(app_settings):
    # Setup defaults if setting did not load/exist
    if "-THEME-" not in app_settings:
        app_settings["-THEME-"] = "Light"
    if "-AVG_PERIOD_1-" not in app_settings:
        app_settings["-AVG_PERIOD_1-"] = "4"
    if "-AVG_PERIOD_2-" not in app_settings:
        app_settings["-AVG_PERIOD_2-"] = "8"
    if "-PERIOD_1_WEIGHT-" not in app_settings:
        app_settings["-PERIOD_1_WEIGHT-"] = "25"
    if "-PERIOD_2_WEIGHT-" not in app_settings:
        app_settings["-PERIOD_2_WEIGHT-"] = "75"
    if "-TOP_X-" not in app_settings:
        app_settings["-TOP_X-"] = "5"
    if "-CALC_TYPE-" not in app_settings:
        app_settings["-CALC_TYPE-"] = "PCR"
    if "-AGG_TYPE-" not in app_settings:
        app_settings["-AGG_TYPE-"] = "Monthly"
    if "-OPEN_FILES-" not in app_settings:
        app_settings["-OPEN_FILES-"] = False
    if "-BACKTEST-" not in app_settings:
        app_settings["-BACKTEST-"] = False
    if "-START_VALUE-" not in app_settings:
        app_settings["-START_VALUE-"] = "100000"
    if "-START_DATE-" not in app_settings:
        app_settings["-START_DATE-"] = ""
    if "-END_DATE-" not in app_settings:
        app_settings["-END_DATE-"] = ""
    if "-EXPORT-" not in app_settings:
        app_settings["-EXPORT-"] = False
    if "-EXPORT_OO_SIG-" not in app_settings:
        app_settings["-EXPORT_OO_SIG-"] = False
    if "-SCALING-" not in app_settings:
        app_settings["-SCALING-"] = False
    if "-MIN_TRANCHES-" not in app_settings:
        app_settings["-MIN_TRANCHES-"] = "5"
    if "-MAX_TRANCHES-" not in app_settings:
        app_settings["-MAX_TRANCHES-"] = "5"
    if "-BP_PER-" not in app_settings:
        app_settings["-BP_PER-"] = "6000"
    if "-PORTFOLIO_MODE-" not in app_settings:
        app_settings["-PORTFOLIO_MODE-"] = False
    if "-TOP_TIME_THRESHOLD-" not in app_settings:
        app_settings["-TOP_TIME_THRESHOLD-"] = ""
    if "-TIME_PER_TEST-" not in app_settings:
        app_settings["-TIME_PER_TEST-"] = "15"


def update_strategy_settings(values, settings):
    settings.update(
        {
            "-AVG_PERIOD_1-": values["-AVG_PERIOD_1-"],
            "-PERIOD_1_WEIGHT-": values["-PERIOD_1_WEIGHT-"],
            "-AVG_PERIOD_2-": values["-AVG_PERIOD_2-"],
            "-PERIOD_2_WEIGHT-": values["-PERIOD_2_WEIGHT-"],
            "-TOP_X-": values["-TOP_X-"],
            "-CALC_TYPE-": values["-CALC_TYPE-"],
            "-AGG_TYPE-": values["-AGG_TYPE-"],
            "-MIN_TRANCHES-": values["-MIN_TRANCHES-"],
            "-MAX_TRANCHES-": values["-MAX_TRANCHES-"],
            "-BP_PER-": values["-BP_PER-"],
            "-PASSTHROUGH_MODE-": values["-PASSTHROUGH_MODE-"],
            "-PORT_WEIGHT-": values["-PORT_WEIGHT-"],
            "-TOP_TIME_THRESHOLD-": values["-TOP_TIME_THRESHOLD-"],
        }
    )

    # Initialize option settings if they don't exist
    for option in [
        "-WEEKDAY_EXCLUSIONS-",
        "-NEWS_EXCLUSIONS-",
        "-PUT_OR_CALL-",
        "-IDV_WEEKDAY-",
        "-AUTO_EXCLUSIONS-",
        "-GAP_ANALYSIS-",
    ]:
        if option not in settings:
            settings[option] = []
    if "-APPLY_EXCLUSIONS-" not in settings:
        settings["-APPLY_EXCLUSIONS-"] = "Both"
    if "-GAP_THRESHOLD-" not in settings:
        settings["-GAP_THRESHOLD-"] = 0
    if "-GAP_TYPE-" not in settings:
        settings["-GAP_TYPE-"] = "%"


def validate_strategy_settings(strategy_settings):
    for strategy in strategy_settings:
        try:
            period1 = int(strategy_settings[strategy]["-AVG_PERIOD_1-"])
            period2 = int(strategy_settings[strategy]["-AVG_PERIOD_2-"])
            weight1 = float(strategy_settings[strategy]["-PERIOD_1_WEIGHT-"])
            weight2 = float(strategy_settings[strategy]["-PERIOD_2_WEIGHT-"])
            strategy_settings[strategy]["-AVG_PERIOD_1-"] = period1
            strategy_settings[strategy]["-AVG_PERIOD_2-"] = period2
            strategy_settings[strategy]["-PERIOD_1_WEIGHT-"] = weight1
            strategy_settings[strategy]["-PERIOD_2_WEIGHT-"] = weight2

            strategy_settings[strategy]["-TOP_X-"] = int(
                strategy_settings[strategy]["-TOP_X-"]
            )
            strategy_settings[strategy]["-MIN_TRANCHES-"] = int(
                strategy_settings[strategy]["-MIN_TRANCHES-"]
            )
            strategy_settings[strategy]["-MAX_TRANCHES-"] = int(
                strategy_settings[strategy]["-MAX_TRANCHES-"]
            )
            strategy_settings[strategy]["-BP_PER-"] = float(
                strategy_settings[strategy]["-BP_PER-"]
            )
            strategy_settings[strategy]["-PORT_WEIGHT-"] = float(
                strategy_settings[strategy]["-PORT_WEIGHT-"]
            )
            if strategy_settings[strategy]["-TOP_TIME_THRESHOLD-"]:
                strategy_settings[strategy]["-TOP_TIME_THRESHOLD-"] = float(
                    strategy_settings[strategy]["-TOP_TIME_THRESHOLD-"]
                )
            else:
                strategy_settings[strategy]["-TOP_TIME_THRESHOLD-"] = float("-inf")
        except ValueError:
            return (
                "Problem with values entered.\nPlease enter only positive whole numbers"
            )
        if period1 < 1 or period2 < 1 or period1 > period2:
            return "Please make sure both averaging periods are > 0\nand that Trailing Avg 2 is >= to Trailing Avg 1"
        if weight1 + weight2 != 100:
            return "Trailing Avg Weights should add up to 100"

    return True


@with_gc
def options_window(settings) -> None:
    global news_events_loaded
    dpi_scale = get_dpi_scale()
    BASE_HEIGHT = 40
    scaled_height = int(BASE_HEIGHT * dpi_scale)
    weekday_exclusion_checkboxes = [
        Checkbox(
            day,
            day in settings["-WEEKDAY_EXCLUSIONS-"],
            key=day,
            font=font,
            size=(6, 1),
        )
        for day in weekday_list
    ]
    news_exclusion_checkboxes = [
        Checkbox(
            release,
            release in settings["-NEWS_EXCLUSIONS-"],
            key=release,
            font=font,
            size=(11, 1),
        )
        for release in news_events
    ]
    # break into rows of 3
    news_exclusion_checkboxes = chunk_list(news_exclusion_checkboxes, 3)
    fxstreet_link = "https://www.fxstreet.com/economic-calendar"
    donate_paypal_link = "https://www.paypal.com/donate?hosted_button_id=ZDZEXUHMZR9RJ"
    donate_venmo_link = "https://www.venmo.com/u/excessivechaos"
    layout = [
        [
            sg.Text("Economic Calendar CSV file (", pad=(0, 0)),
            sg.Text(
                fxstreet_link,
                pad=(0, 0),
                enable_events=True,
                font=font + ("underline",),
                text_color="blue",
                key=("-LINK-", fxstreet_link),
            ),
            sg.Text(")", pad=(0, 0)),
        ],
        [
            sg.Input(
                "Loaded" if news_events_loaded else "",
                key="-FILE-",
                expand_x=True,
            ),
            sg.Button("Browse"),
        ],
        [
            sg.Frame(
                "Exclude Weekday",
                [weekday_exclusion_checkboxes],
                expand_x=True,
            ),
        ],
        [
            sg.Frame(
                "Exclude news",
                news_exclusion_checkboxes,
                expand_x=True,
            ),
        ],
        [
            sg.Frame(
                "Analysis Options",
                [
                    [
                        sg.Text("Apply Exclusions to:", pad=((5, 1), 5)),
                        sg.Combo(
                            ["Both", "Analysis", "Walk Forward Test"],
                            settings["-APPLY_EXCLUSIONS-"],
                            key="-APPLY_EXCLUSIONS-",
                            font=font,
                            tooltip="Apply the excluded events/weekdays to the analysis\nof best times, or just the walk-forward test, or both.",
                            pad=(0, 5),
                            readonly=True,
                        ),
                    ],
                    [
                        Checkbox(
                            "Put or Call",
                            settings["-PUT_OR_CALL-"],
                            key="-PUT_OR_CALL-",
                            font=font,
                            size=(6, 1),
                            tooltip="Compare selecting the best times to trade only puts or calls",
                        ),
                        Checkbox(
                            "Individual Weekday",
                            settings["-IDV_WEEKDAY-"],
                            key="-IDV_WEEKDAY-",
                            font=font,
                            size=(10, 1),
                            tooltip="Compare selecting the best times for each specific weekday to trade for that weekday",
                        ),
                        Checkbox(
                            "Auto Exclusions",
                            False,  # settings["-AUTO_EXCLUSIONS-"],
                            key="-AUTO_EXCLUSIONS-",
                            font=font,
                            size=(10, 1),
                            pad=(5, 5),
                            tooltip="Allow Walk-Forward test to determine which events to exclude\nbased on whether the event has -EV from prior lookback period.\nNote: This will require an additional warmup period.",
                            visible=False,  # disable this setting for now, not too useful.
                        ),
                    ],
                    [sg.HorizontalSeparator()],
                    [
                        Checkbox(
                            "Use Gap Analysis",
                            settings["-GAP_ANALYSIS-"],
                            key="-GAP_ANALYSIS-",
                            font=font,
                            size=(10, 1),
                            tooltip="Look for the best times for gap up and down days",
                        ),
                        sg.Text("Threshold:"),
                        sg.Input(
                            settings["-GAP_THRESHOLD-"],
                            size=(5, 1),
                            key="-GAP_THRESHOLD-",
                        ),
                        sg.Combo(
                            ["%", "Points"],
                            settings["-GAP_TYPE-"],
                            readonly=True,
                            key="-GAP_TYPE-",
                        ),
                    ],
                ],
                expand_x=True,
            ),
        ],
        [
            sg.Ok(),
            sg.Cancel(),
            sg.Push(),
            sg.Button(
                image_data=resize_base64_image(
                    donate_paypal_logo, int(scaled_height * 1.5)
                ),
                button_color=sg.theme_background_color(),
                border_width=0,
                key=("-LINK-", donate_paypal_link),
                pad=(0, 0),
            ),
            sg.Button(
                image_data=resize_base64_image(donate_venmo_logo, scaled_height),
                button_color=sg.theme_background_color(),
                border_width=0,
                key=("-LINK-", donate_venmo_link),
                pad=(0, 0),
            ),
        ],
    ]

    window = sg.Window(
        "Options",
        layout,
        no_titlebar=False,
        # size=window_size,
        finalize=True,
        modal=True,
        resizable=True,
    )
    Checkbox.initial(window)
    # let window be made so the length is auto set
    # the width always fills the screen when using the custom
    # checkbox class, so we need to change the size.  Allowing
    # the window to self size first we can get the correct height
    window_height = window.size[1]
    window_width = int(650 * dpi_scale)
    window.TKroot.geometry(f"{window_width}x{window_height}")

    # Set the window position
    window.move_to_center()

    while True:
        event, values = window.read()
        if event in (sg.WIN_CLOSED, "Cancel"):
            break

        elif event == "Browse":
            news_file = sg.popup_get_file(
                "",
                file_types=(("CSV Files", "*.csv"),),
                multiple_files=False,
                no_window=True,
            )
            window["-FILE-"].update(news_file)

        elif event == "Ok":
            settings["-WEEKDAY_EXCLUSIONS-"] = [
                day for day in weekday_list if values[day]
            ]
            settings["-NEWS_EXCLUSIONS-"] = [
                release for release in news_events if values[release]
            ]
            settings["-PUT_OR_CALL-"] = values["-PUT_OR_CALL-"]
            settings["-IDV_WEEKDAY-"] = values["-IDV_WEEKDAY-"]
            settings["-GAP_ANALYSIS-"] = values["-GAP_ANALYSIS-"]
            try:
                settings["-GAP_THRESHOLD-"] = float(values["-GAP_THRESHOLD-"])
            except ValueError:
                sg.popup_no_border("Please correct Gap Threshold value")
                continue
            settings["-GAP_TYPE-"] = values["-GAP_TYPE-"]
            settings["-APPLY_EXCLUSIONS-"] = values["-APPLY_EXCLUSIONS-"]
            settings["-AUTO_EXCLUSIONS-"] = values["-AUTO_EXCLUSIONS-"]
            if values["-FILE-"] and values["-FILE-"] != "Loaded":
                result = import_news_events(values["-FILE-"])
                if result:
                    results_queue.put(("-IMPORT_NEWS-", result))
                    news_events_loaded = True
                    break
                else:
                    sg.popup_no_border(
                        "This does not appear to be a CSV from\nhttps://www.fxstreet.com/economic-calendar"
                    )
                    continue
            else:
                break

        elif event[0] == "-LINK-":
            webbrowser.open(event[1])
    window.close()
    Checkbox.clear_elements()


@with_gc
def optimizer_window(files_list, app_settings, strategy_settings) -> None:
    path = os.path.dirname(files_list[0])
    files = [os.path.basename(file) for file in files_list]
    dpi_scale = get_dpi_scale()
    time_per_test = float(app_settings["-TIME_PER_TEST-"])
    text = (
        "This will perform an optimization of TTA settings using a genetic algorithm. "
        "First, n parents will be created using randomly select settings/traits. The "
        "walk-forward test will be performed for each configuration. "
        "These parents will spawn several children "
        "each inheriting their traits from the parent. Next the children will have one "
        "or more of their traits mutated with a new random value. The top performer "
        "from the child generation will then be chosen as parent to the next generation. "
        "This will continue for however many generations are chosen. Use the selection "
        "preference to set the metric (e.g. MAR, Sharpe, CAGR) that will be used for "
        "determining the top performers. At the the end of the test, the top settings "
        "will be set in the main window and a final WF test run to display the charts "
    )
    wrapped_text = textwrap.fill(text, width=140)
    layout = [
        [sg.Text(wrapped_text, font=font)],
        [
            sg.Text(
                (
                    "Warning: This optimization is very time consuming,"
                    + "\nespecially with a high number of generations or children"
                ),
                font=(font[0], font[1] + 2),
                text_color="red",
            )
        ],
        [
            sg.Text("File to optimize:", font=font),
            sg.Combo(
                files,
                default_value=files[0],
                key="-FILE-",
                readonly=True,
                font=font,
                expand_x=True,
            ),
        ],
        [
            sg.Text("Number of Initial Parents:", font=font, size=(23, 1)),
            sg.Input(default_text="10", key="-PARENTS-", size=(5, 1), font=font),
            sg.Text("Number of Generations:", font=font),
            sg.Input(default_text="10", key="-GENERATIONS-", size=(5, 1), font=font),
            sg.Text("Number of Children:", font=font),
            sg.Input(default_text="5", key="-CHILDREN-", size=(5, 1), font=font),
        ],
        [
            sg.Text("Max Tranches:", font=font, size=(23, 1)),
            sg.Input(default_text="15", key="-MAX_TRANCHES-", size=(5, 1), font=font),
            sg.Text("Weight Steps:", font=font),
            sg.Input(default_text="5", key="-WEIGHT_STEPS-", size=(5, 1), font=font),
            sg.Text("(e.g. 5 = weights of 5, 10, 15...)", font=font, size=(23, 1)),
        ],
        [
            sg.Text("Selection Preference:", font=font, size=(23, 1)),
            sg.Combo(
                [
                    "MAR",
                    "Sharpe",
                    "CAGR",
                    "Drawdown%",
                    "Days in Drawdown",
                    "Total Return",
                    "Largest Month",
                    "Smallest Month",
                ],
                default_value="MAR",
                key="-SELECTION_METRIC-",
                readonly=True,
                size=(15, 1),
                font=font,
            ),
        ],
        [
            sg.Frame(
                "Static Settings",
                [
                    [
                        sg.Text(
                            (
                                "These Settings will be made static, thus reducing the possible combinations."
                                "\nWhen applied the value that is currently set in the main and options windows will be used."
                            ),
                            font=font,
                        )
                    ],
                    [
                        Checkbox(
                            "Tranche Qty",
                            False,
                            key="-TOP_X-",
                            font=font,
                            size=(6, 1),
                        ),
                        Checkbox(
                            "Calc Type",
                            False,
                            key="-CALC_TYPE-",
                            font=font,
                            size=(5, 1),
                        ),
                        Checkbox(
                            "Aggregation Type",
                            False,
                            key="-AGG_TYPE-",
                            font=font,
                            size=(10, 1),
                        ),
                        Checkbox(
                            "Put or Call",
                            False,
                            key="-PUT_OR_CALL-",
                            font=font,
                            size=(6, 1),
                        ),
                        Checkbox(
                            "Individual Weekday",
                            False,
                            key="-IDV_WEEKDAY-",
                            font=font,
                            size=(10, 1),
                        ),
                        Checkbox(
                            "Use only 1 Avg Period",
                            False,
                            key="-USE_1_AVG-",
                            font=font,
                            size=(11, 1),
                        ),
                    ],
                ],
                expand_x=True,
            )
        ],
        [
            Checkbox(
                "Exhaustive Search",
                False,
                key="-USE_EXHAUSTIVE-",
                font=font,
                size=(11, 1),
            ),
        ],
        [
            sg.Text(
                (
                    "A genetic search is more appropriate for searching a large population, when testing all possibilities would be too time consuming."
                    "\nAn exhaustive search is appropriate for a smaller population, when test all possible combinations is achievable in a shorter timeframe."
                ),
                font=font,
            ),
        ],
        [sg.HorizontalSeparator()],
        [
            sg.Text("Total Tests to Run:", font=font),
            sg.Text("", font=font, key="-TOTAL_TESTS-"),
            sg.Text("Out of a possible:", font=font),
            sg.Text("", font=font, key="-TOTAL_POSSIBLE-"),
            sg.Text("combinations", font=font),
        ],
        [
            sg.Text("Estimated Run Time:", font=font),
            sg.Text("", font=font, key="-RUN_TIME-"),
        ],
        [
            sg.Text(
                (
                    "Note: Estimated time will improve after each run. It is helpful to perform a quick"
                    "\ninitial test with 10 parents, 1 generation, and 1 child to get a better time estimate."
                ),
                font=font,
            )
        ],
        [sg.Button("Start", font=font), sg.Button("Cancel", font=font)],
    ]

    window = sg.Window(
        "Optimizer",
        layout,
        no_titlebar=False,
        # size=window_size,
        finalize=True,
        modal=True,
        resizable=True,
    )
    Checkbox.initial(window)
    # let window be made so the length is auto set
    # the width always fills the screen when using the custom
    # checkbox class, so we need to change the size.  Allowing
    # the window to self size first we can get the correct height
    window_height = window.size[1]
    window_width = int(900 * dpi_scale)
    window.TKroot.geometry(f"{window_width}x{window_height}")

    # Now we need to move the window since it opens all the way left.
    window.move_to_center()
    optimizer_thread = None
    while True:
        event, values = window.read(timeout=100)
        if event in (sg.WIN_CLOSED, "Cancel"):
            break

        elif event == "Start":
            try:
                generations = int(values["-GENERATIONS-"])
                children = int(values["-CHILDREN-"])
                parents = int(values["-PARENTS-"])
                max_tranches = int(values["-MAX_TRANCHES-"])
                weight_steps = int(values["-WEIGHT_STEPS-"])
            except ValueError:
                sg.popup_no_border(
                    "Please correct Inputs. Only integers are allowed.", font=font
                )
                continue
            selected_file = values["-FILE-"]
            if app_settings["-PORTFOLIO_MODE-"]:
                selected_strat_settings = strategy_settings[selected_file]
            else:
                selected_strat_settings = strategy_settings["-SINGLE_MODE-"]
            static_settings = {
                "-START_VALUE-": app_settings["-START_VALUE-"],
                "-BP_PER-": selected_strat_settings["-BP_PER-"],
            }
            for key in [
                "-TOP_X-",
                "-CALC_TYPE-",
                "-AGG_TYPE-",
                "-PUT_OR_CALL-",
                "-IDV_WEEKDAY-",
            ]:
                if values[key]:
                    static_settings[key] = selected_strat_settings[key]
            if values["-USE_1_AVG-"]:
                static_settings["-AVG_PERIOD_2-"] = 12
                static_settings["-PERIOD_2_WEIGHT-"] = 0

            if not values["-USE_EXHAUSTIVE-"]:
                optimizer_thread = threading.Thread(
                    target=genetic_optimizer,
                    kwargs={
                        "file": os.path.join(path, selected_file),
                        "generations": generations,
                        "children": children,
                        "num_parents": parents,
                        "selection_metric": values["-SELECTION_METRIC-"],
                        "cancel_flag": cancel_flag,
                        "results_queue": results_queue,
                        "weekday_list": weekday_list,
                        "news_events": news_events,
                        "static_settings": static_settings,
                        "max_tranches": max_tranches,
                        "bp_per": float(app_settings["-BP_PER-"]),
                        "initial_value": float(app_settings["-START_VALUE-"]),
                        "weight_steps": weight_steps,
                    },
                )
            else:
                optimizer_thread = threading.Thread(
                    target=exhaustive_optimizer,
                    kwargs={
                        "file": os.path.join(path, selected_file),
                        "selection_metric": values["-SELECTION_METRIC-"],
                        "cancel_flag": cancel_flag,
                        "results_queue": results_queue,
                        "weekday_list": weekday_list,
                        "news_events": news_events,
                        "static_settings": static_settings,
                        "max_tranches": max_tranches,
                        "bp_per": float(app_settings["-BP_PER-"]),
                        "initial_value": float(app_settings["-START_VALUE-"]),
                        "weight_steps": weight_steps,
                    },
                )
            optimizer_thread.start()
            break
        try:
            generations = int(values["-GENERATIONS-"])
            children = int(values["-CHILDREN-"])
            parents = int(values["-PARENTS-"])
            max_tranches = int(values["-MAX_TRANCHES-"])
            weight_steps = int(values["-WEIGHT_STEPS-"])

            if values["-USE_1_AVG-"]:
                num_weights = 1
                num_months = 12
            else:
                num_weights = int(100 / weight_steps)
                num_months = (
                    78  # total combos of 2 avg periods from 1 to 12, 2nd >= first
                )
            num_tranches = max_tranches if not values["-TOP_X-"] else 1
            num_calc_types = 2 if not values["-CALC_TYPE-"] else 1
            num_agg_types = 3 if not values["-AGG_TYPE-"] else 1
            num_PorC = 2 if not values["-PUT_OR_CALL-"] else 1
            num_idv_weekday = 2 if not values["-IDV_WEEKDAY-"] else 1
            total_possible = (
                num_weights
                * num_months
                * num_tranches
                * num_calc_types
                * num_agg_types
                * num_PorC
                * num_idv_weekday
            )
            window["-TOTAL_POSSIBLE-"].update(total_possible)

            if values["-USE_EXHAUSTIVE-"]:
                total_tests = total_possible
            else:
                total_tests = int(values["-GENERATIONS-"]) * int(
                    values["-CHILDREN-"]
                ) * int(values["-PARENTS-"]) + int(values["-PARENTS-"])
            window["-TOTAL_TESTS-"].update(total_tests)

            total_time = total_tests * time_per_test
            secs = total_time % 60
            mins = (total_time // 60) % 60
            hours = total_time // 3600
            window["-RUN_TIME-"].update(f"{hours:.0f}h:{mins:.0f}m:{secs:.0f}s")

        except ValueError:
            window["-TOTAL_TESTS-"].update("")
            window["-RUN_TIME-"].update("")
            window["-TOTAL_POSSIBLE-"].update("")
    window.close()
    Checkbox.clear_elements()
    return optimizer_thread


def main():
    global news_events_loaded, news_events, logger
    logger = setup_logging(logger, "ERROR")
    # try to load news events if csv found
    find_news_process = threading.Thread(
        target=find_and_import_news_events, args=(results_queue,), daemon=True
    )
    find_news_process.start()
    # load default settings or last used
    app_settings = {}
    settings_filename = os.path.join(os.path.curdir, "data", "tta_settings.json")
    if os.path.exists(settings_filename):
        try:
            with open(settings_filename, "r") as f:
                app_settings = json.load(f)
        except json.JSONDecodeError:
            # delete bad file
            results_queue.put(
                (
                    "-ERROR-",
                    "Error loading tta_settings.json\nDeleting corrupt file and\nfalling back to defaults",
                )
            )
            try:
                os.remove(settings_filename)
            except:
                results_queue.put(
                    (
                        "-ERROR-",
                        "Could not delete corrupt tta_settings.json file.\nPlease remove manually from data directory",
                    )
                )
                pass
    # setup defaults if setting did not load/exist
    set_default_app_settings(app_settings)

    sg.theme(themes[app_settings["-THEME-"]])
    sg.theme_button_color(button_color)  # override button color

    def get_main_window(values=None, old_window=None):
        tg_strat_layout = []
        for tg_strat in ["Put-Call Comb", "Best P/C", "Puts", "Calls"]:
            tg_gap_layout = []
            for tg_gap in ["All", "Gap Up", "Gap Down"]:
                tg_day_layout = []
                for day in ["All", "Mon", "Tue", "Wed", "Thu", "Fri"]:
                    tab = sg.Tab(
                        day,
                        [
                            [
                                sg.Table(
                                    (
                                        old_window.key_dict[
                                            f"-TABLE_{tg_strat}_{tg_gap}_{day}-"
                                        ].Values
                                        if old_window
                                        else ""
                                    ),
                                    ["Top Times", "Avg", "Source File"],
                                    key=f"-TABLE_{tg_strat}_{tg_gap}_{day}-",
                                    expand_x=True,
                                    auto_size_columns=True,
                                    # background_color="white",
                                    # alternating_row_color="darkgrey",
                                    # header_text_color="black",
                                    # header_background_color="lightblue",
                                )
                            ]
                        ],
                        expand_x=True,
                    )
                    tg_day_layout.append(tab)
                gap_group_tab = sg.Tab(
                    tg_gap,
                    [[sg.TabGroup([tg_day_layout], expand_x=True)]],
                    expand_x=True,
                )
                tg_gap_layout.append(gap_group_tab)
            main_group_tab = sg.Tab(
                tg_strat,
                [[sg.TabGroup([tg_gap_layout], expand_x=True)]],
                expand_x=True,
            )
            tg_strat_layout.append(main_group_tab)

        chart_tab = sg.Tab(
            "Charts",
            [
                [
                    sg.TabGroup(
                        [
                            [
                                sg.Tab(
                                    "PnL",
                                    [
                                        [
                                            sg.Table(
                                                (
                                                    old_window.key_dict[
                                                        "-PNL_TABLE_CHART-"
                                                    ].Values
                                                    if old_window
                                                    else ""
                                                ),
                                                [
                                                    "Strategy",
                                                    "Final Value",
                                                    "Profit",
                                                    "Total Return",
                                                    "CAGR",
                                                    "Max DD",
                                                    "Max DD Days",
                                                    "W Strk",
                                                    "L Strk",
                                                    "High Month",
                                                    "Low Month",
                                                    "MAR",
                                                    "Sharpe",
                                                ],
                                                key="-PNL_TABLE_CHART-",
                                                expand_x=True,
                                                num_rows=4,
                                                auto_size_columns=True,
                                                # background_color="lightgrey",
                                                # alternating_row_color="darkgrey",
                                                # header_text_color="black",
                                                # header_background_color="lightblue",
                                            )
                                        ],
                                        [
                                            sg.Image(
                                                key="-PNL_CHART-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                ),
                                sg.Tab(
                                    "PnL by Weekday",
                                    [
                                        [
                                            sg.Image(
                                                key="-WEEKDAY_PNL_CHART-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                ),
                                sg.Tab(
                                    "Monthly PnL",
                                    [
                                        [
                                            sg.Image(
                                                key="-MONTHLY_PNL_CHART-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                ),
                                sg.Tab(
                                    "PnL by News Event",
                                    [
                                        [
                                            sg.Image(
                                                key="-NEWS_PNL_CHART-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                ),
                                sg.Tab(
                                    "Avg PnL per News Event",
                                    [
                                        [
                                            sg.Image(
                                                key="-NEWS_AVG_PNL_CHART-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                ),
                                sg.Tab(
                                    "Correlation Matrix",
                                    [
                                        [
                                            sg.Image(
                                                key="-CORRELATION_MATRIX-",
                                                size=(
                                                    int(screen_size[0] * 0.25),
                                                    int(screen_size[1] * 0.25),
                                                ),
                                                expand_x=True,
                                                expand_y=True,
                                            )
                                        ],
                                    ],
                                    key="-CORRELATION_MATRIX_TAB-",
                                    visible=(
                                        old_window["-CORRELATION_MATRIX_TAB-"].visible
                                        if old_window
                                        else False
                                    ),
                                ),
                            ]
                        ],
                        expand_x=True,
                        expand_y=True,
                    )
                ]
            ],
        )
        tg_strat_layout.append(chart_tab)

        layout = [
            [
                sg.Button("Analyze", pad=(5, 10), bind_return_key=True),
                sg.Text("  "),
                sg.pin(
                    sg.ProgressBar(
                        100,
                        orientation="h",
                        size=(50, 30),
                        key="-PROGRESS-",
                        expand_x=True,
                        visible=False,
                    ),
                ),
                sg.pin(sg.Button("Cancel", pad=(20, 0), visible=False)),
                sg.Push(),
                sg.Button("Optimizer"),
                sg.Button("CSV Merger"),
                sg.Combo(
                    list(themes),
                    default_value=app_settings["-THEME-"],
                    key="-THEME-",
                    enable_events=True,
                    readonly=True,
                ),
                sg.Text(__version__),
            ],
            [sg.Text("Select trade log CSV file:")],
            [
                sg.Input(
                    key="-FILE-",
                    expand_x=True,
                ),
                sg.Button("Browse"),
            ],
            [
                Checkbox(
                    "Portfolio Mode",
                    app_settings["-PORTFOLIO_MODE-"],
                    key="-PORTFOLIO_MODE-",
                    size=(12, 1),
                    enable_events=True,
                ),
                sg.pin(
                    sg.Combo(
                        [],
                        key="-STRATEGY_SELECT-",
                        readonly=True,
                        visible=app_settings["-PORTFOLIO_MODE-"],
                        enable_events=True,
                        size=(50, 1),
                    )
                ),
                sg.pin(
                    Checkbox(
                        "Pass-through Mode",
                        False,
                        key="-PASSTHROUGH_MODE-",
                        size=(14, 1),
                        tooltip="This will skip analysis and allow the trades\nto pass-through as is to the walk-forward test.\nThis can be used for adding non-tranche\nstrategies to the portfolio for analysis",
                        visible=app_settings["-PORTFOLIO_MODE-"],
                    )
                ),
                sg.Push(),
                sg.Button("Options", button_color="green"),
            ],
            [
                sg.Frame(
                    "",
                    [
                        [
                            sg.Text(
                                "Trailing Avg 1:",
                                tooltip="Number of months for first averaging period.\nNote: should be the shorter period",
                            ),
                            sg.Input(
                                app_settings["-AVG_PERIOD_1-"],
                                key="-AVG_PERIOD_1-",
                                size=(3, 1),
                                justification="c",
                                tooltip="Number of months for first averaging period.\nNote: should be the shorter period",
                            ),
                            sg.Text("Months "),
                            sg.Text(
                                "Weight:",
                                tooltip="Weight in % for first avg period\nNote: Set to 100 for this and 0 for 2nd if only using 1 period",
                            ),
                            sg.Input(
                                app_settings["-PERIOD_1_WEIGHT-"],
                                key="-PERIOD_1_WEIGHT-",
                                size=(3, 1),
                                justification="c",
                                tooltip="Weight in % for first avg period\nNote: Set to 100 for this and 0 for 2nd if only using 1 period",
                            ),
                            sg.Text("   "),
                            sg.Text(
                                "Trailing Avg 2:",
                                tooltip="Number of months for second averaging period.\nNote: should be the longer period or same as 1",
                            ),
                            sg.Input(
                                app_settings["-AVG_PERIOD_2-"],
                                key="-AVG_PERIOD_2-",
                                size=(3, 1),
                                justification="c",
                                tooltip="Number of months for second averaging period.\nNote: should be the longer period or same as 1",
                            ),
                            sg.Text("Months "),
                            sg.Text(
                                "Weight:",
                                tooltip="Weight in % for second avg period\nNote: Set to 0 to only use the 1st period",
                            ),
                            sg.Input(
                                app_settings["-PERIOD_2_WEIGHT-"],
                                key="-PERIOD_2_WEIGHT-",
                                size=(3, 1),
                                justification="c",
                                tooltip="Weight in % for second avg period\nNote: Set to 0 to only use the 1st period",
                            ),
                        ],
                        [
                            sg.Text(
                                "Select Top",
                                pad=(5, 5),
                                tooltip="Highlight the top n times for each month in the heatmap.\nWill also display the top n times below",
                            ),
                            sg.Input(
                                app_settings["-TOP_X-"],
                                key="-TOP_X-",
                                size=(3, 1),
                                pad=(0, 0),
                                justification="c",
                                tooltip="Highlight the top n times for each month in the heatmap.\nWill also display the top n times below",
                            ),
                            sg.Text("Time Tranches", pad=(5, 0)),
                            sg.Text("Above:"),
                            sg.Input(
                                app_settings["-TOP_TIME_THRESHOLD-"],
                                key="-TOP_TIME_THRESHOLD-",
                                size=(4, 1),
                                justification="c",
                            ),
                            sg.Text(
                                app_settings["-CALC_TYPE-"],
                                key="-CALC_TYPE_TEXT-",
                                pad=(0, 0),
                            ),
                            sg.Text("   Averaging Mode"),
                            sg.Combo(
                                ["PCR", "PnL"],
                                app_settings["-CALC_TYPE-"],
                                key="-CALC_TYPE-",
                                readonly=True,
                                enable_events=True,
                            ),
                            sg.Text(
                                "   Aggregation Period",
                                tooltip="Aggregate the results into monthly averages or weekly\nIf doing a walk-forward test the top times will be updated at this frequency.",
                            ),
                            sg.Combo(
                                ["Monthly", "Semi-Monthly", "Weekly"],
                                app_settings["-AGG_TYPE-"],
                                key="-AGG_TYPE-",
                                tooltip="Aggregate the results into monthly averages or weekly\nIf doing a walk-forward test the top times will be updated at this frequency.",
                                readonly=True,
                            ),
                            sg.Push(),
                            Checkbox(
                                "Open Excel files after creation",
                                app_settings["-OPEN_FILES-"],
                                key="-OPEN_FILES-",
                                size=(20, 1),
                            ),
                        ],
                    ],
                    expand_x=True,
                )
            ],
            [
                sg.Frame(
                    "",
                    [
                        [
                            Checkbox(
                                "Perform walk-forward backtest",
                                app_settings["-BACKTEST-"],
                                key="-BACKTEST-",
                                size=(19, 1),
                                tooltip="Out of sample/walk forward test.  Optimize times for prior lookback period\nand test outcome in the following month (out of sample).\nWalk forward to the next month and re-optimize times.",
                            ),
                            sg.Text(
                                "Starting Value",
                                tooltip="Portfolio Value to start from.  If using scaling the BP per contract\nwill be divided by this amount to determine the number of contracts to trade",
                            ),
                            sg.Input(
                                app_settings["-START_VALUE-"],
                                size=(10, 1),
                                key="-START_VALUE-",
                                justification="r",
                                tooltip="Portfolio Value to start from.  If using scaling the BP per contract\nwill be divided by this amount to determine the number of contracts to trade",
                            ),
                            sg.Text(
                                "   Start Date",
                                tooltip="Date to start test from. Leave blank to automatically\nselect the earliest available start date from the available data",
                            ),
                            sg.Input(
                                app_settings["-START_DATE-"],
                                key="-START_DATE-",
                                size=(12, 1),
                                justification="c",
                                tooltip="Date to start test from. Leave blank to automatically\nselect the earliest available start date from the available data",
                            ),
                            sg.Text(
                                " End Date",
                                tooltip="Date to end test. Leave blank to automatically\nselect the latest available end date from the available data",
                            ),
                            sg.Input(
                                app_settings["-END_DATE-"],
                                key="-END_DATE-",
                                size=(12, 1),
                                justification="c",
                                tooltip="Date to end test. Leave blank to automatically\nselect the latest available end date from the available data",
                            ),
                            sg.Push(),
                            Checkbox(
                                "Export Trades to CSV",
                                app_settings["-EXPORT-"],
                                key="-EXPORT-",
                                size=(16, 1),
                            ),
                        ],
                        [
                            Checkbox(
                                "Use Scaling",
                                app_settings["-SCALING-"],
                                key="-SCALING-",
                                size=(10, 1),
                                tooltip="Uses scaling logic to determine the number of contracts\nto trade each day of the backtest based on current portfolio value\nand the BP per contract.",
                            ),
                            sg.Text(
                                "Min Tranches",
                                tooltip="When using scaling, this the minimum number of tranche times",
                            ),
                            sg.Input(
                                app_settings["-MIN_TRANCHES-"],
                                key="-MIN_TRANCHES-",
                                size=(3, 1),
                                justification="c",
                                tooltip="When using scaling, this the minimum number of tranche times",
                            ),
                            sg.Text(
                                "   Max Tranches",
                                tooltip="When using scaling, this the maximum number of tranche times.\nAdditional contracts over this amount will be distributed among the available tranche times.",
                            ),
                            sg.Input(
                                app_settings["-MAX_TRANCHES-"],
                                key="-MAX_TRANCHES-",
                                size=(3, 1),
                                justification="c",
                                tooltip="When using scaling, this the maximum number of tranche times.\nAdditional contracts over this amount will be distributed among the available tranche times.",
                            ),
                            sg.Text(
                                "   BP Per Contract",
                                tooltip="Amount of buying power to use for each contract.  This is only used to determine\nthe total number of contracts to trade each day when using scaling.",
                            ),
                            sg.Input(
                                app_settings["-BP_PER-"],
                                key="-BP_PER-",
                                size=(6, 1),
                                justification="r",
                                tooltip="Amount of buying power to use for each contract.  This is only used to determine\nthe total number of contracts to trade each day when using scaling.",
                            ),
                            sg.pin(
                                sg.Text(
                                    "Portfolio Weight",
                                    visible=app_settings["-PORTFOLIO_MODE-"],
                                    key="-PORT_WEIGHT_TEXT1-",
                                )
                            ),
                            sg.pin(
                                sg.Input(
                                    "100",
                                    key="-PORT_WEIGHT-",
                                    size=(5, 1),
                                    justification="c",
                                    tooltip="The weight the selected strategy will have in the portfolio rebalanced daily.",
                                    visible=app_settings["-PORTFOLIO_MODE-"],
                                )
                            ),
                            sg.pin(
                                sg.Text(
                                    "%",
                                    pad=(0, 0),
                                    visible=app_settings["-PORTFOLIO_MODE-"],
                                    key="-PORT_WEIGHT_TEXT2-",
                                )
                            ),
                            sg.Push(),
                            Checkbox(
                                "Create OO Signal File",
                                app_settings["-EXPORT_OO_SIG-"],
                                key="-EXPORT_OO_SIG-",
                                size=(16, 1),
                            ),
                        ],
                    ],
                    expand_x=True,
                )
            ],
            [
                sg.TabGroup(
                    [tg_strat_layout],
                    expand_x=True,
                    key="-TAB_GROUP-",
                )
            ],
        ]
        if old_window:
            # create new window with same size and location
            window_size = old_window.size
            window_position = old_window.current_location(False)
        else:
            window_size = (int(screen_size[0] * 0.7), int(screen_size[1] * 0.8))
            window_position = (None, None)
        window = sg.Window(
            "Tranche Time Analyzer",
            layout,
            size=window_size,
            resizable=True,
            finalize=True,
            location=window_position,
        )
        window["-PROGRESS-"].Widget.config(mode="indeterminate")
        Checkbox.initial(window)

        # reselect previously selected tabs
        if old_window:
            # get the currently selected tab
            tab_group = old_window["-TAB_GROUP-"]
            selected = tab_group.get()
            selected_id = [
                "Put-Call Comb",
                "Best P/C",
                "Puts",
                "Calls",
                "Charts",
            ].index(selected)
            window["-TAB_GROUP-"].Widget.select(selected_id)

        # If we have previous values, update the window
        if values:
            for key in values:
                if key in window.AllKeysDict:
                    element = window[key]
                    if isinstance(element, sg.Table):
                        # For Table elements, we need to update the values differently
                        data = old_window.key_dict[key].Values
                        if key == "-PNL_TABLE_CHART-":
                            element.update(values=data, num_rows=min(len(data), 4))
                        else:
                            element.update(values=data, num_rows=len(data))

                    elif isinstance(element, sg.Checkbox):
                        # For Checkbox elements, we need to use the 'value' parameter
                        element.update(value=values[key])
                    elif not isinstance(element, sg.TabGroup):
                        # For most other elements, we can use the 'value' parameter
                        if key == "-STRATEGY_SELECT-":
                            element.update(values=old_window[key].Values)
                        try:
                            element.update(value=values[key])
                        except:
                            pass
        return window

    window = get_main_window()
    error = False
    chart_images = {}
    strategy_settings = {}
    test_running = False
    while True:
        event, values = window.read(timeout=100)
        if event == sg.WIN_CLOSED:
            break
        elif event == "Cancel" and test_running:
            # button will not do anything for normal analysis
            cancel_flag.set()
            window["Cancel"].update("Canceling...", disabled=True)

        elif event == "CSV Merger":
            csv_merger_window()

        elif event == "Options":
            if values["-PORTFOLIO_MODE-"]:
                selected_strategy = values["-STRATEGY_SELECT-"]
            else:
                selected_strategy = "-SINGLE_MODE-"
            if selected_strategy:
                options_window(strategy_settings[selected_strategy])
            else:
                sg.popup_no_border("Please select a strategy first")

        elif event == "Analyze":
            files_list = values["-FILE-"].split(";")
            if "" in files_list:
                files_list.remove("")
            for file in files_list:
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext != ".csv":
                    sg.popup_no_border(
                        "One or more of the selected files\ndo not appear to be a csv file!"
                    )
                    error = True
                    break
            if error:
                error = False  # reset
                continue
            if not files_list:
                sg.popup_no_border("Please Browse for a file first")
                continue
            if values["-PORTFOLIO_MODE-"]:
                selected_strategy = values["-STRATEGY_SELECT-"]
            else:
                selected_strategy = "-SINGLE_MODE-"

            # Save current settings for the selected strategy
            if selected_strategy not in strategy_settings:
                strategy_settings[selected_strategy] = {}
            update_strategy_settings(values, strategy_settings[selected_strategy])

            # Validate settings for all strategies
            result = validate_strategy_settings(strategy_settings)
            if type(result) == str:
                # there was an error
                sg.popup_no_border(result)
                continue

            start_date_str = values["-START_DATE-"]
            end_date_str = values["-END_DATE-"]
            if start_date_str:
                try:
                    start_date = parser.parse(start_date_str, fuzzy=True).date()
                except ValueError:
                    sg.popup_no_border(
                        "Problem parsing Start Date.\nTry entering in YYYY-MM-DD format"
                    )
                    continue
            else:
                start_date = None
            if end_date_str:
                try:
                    end_date = parser.parse(end_date_str, fuzzy=True).date()
                except ValueError:
                    sg.popup_no_border(
                        "Problem parsing End Date.\nTry entering in YYYY-MM-DD format"
                    )
                    continue
            else:
                end_date = None

            # All settings validated, proceed with analysis
            window["-PROGRESS-"].update(visible=True)
            window["Analyze"].update("Working...", disabled=True)
            window["Cancel"].update(visible=True)
            run_analysis_process = Process(
                target=run_analysis_threaded,
                kwargs={
                    "files_list": files_list,
                    "strategy_settings": strategy_settings,
                    "open_files": values["-OPEN_FILES-"],
                    "results_queue": results_queue,
                    "cancel_flag": cancel_flag,
                    "create_excel": True,
                    "news_events": news_events,
                },
            )
            run_analysis_process.start()
            test_running = True

            save_settings(app_settings, settings_filename, values)

        elif event == "Browse":
            files = sg.popup_get_file(
                "",
                file_types=(("CSV Files", "*.csv"),),
                multiple_files=True,
                no_window=True,
                files_delimiter=";",
            )
            if not files:
                # user hit cancel
                continue

            if type(files) == tuple:
                file_str = ";".join(files)
            else:
                file_str = files
            window["-FILE-"].update(file_str)

            strategy_settings.clear()  # reset strategy settings
            if values["-PORTFOLIO_MODE-"]:
                strategies = [os.path.basename(file) for file in file_str.split(";")]
                window["-STRATEGY_SELECT-"].update(values=strategies)
                if strategies:
                    # select the first strategy in the list
                    window["-STRATEGY_SELECT-"].update(value=strategies[0])

            else:
                strategies = ["-SINGLE_MODE-"]
                window["-STRATEGY_SELECT-"].update(values=[])

            # Initialize settings for each strategy
            for strategy in strategies:
                strategy_settings[strategy] = {}
                update_strategy_settings(values, strategy_settings[strategy])
                # set the portfolio weightings to equal weight
                strategy_settings[strategy]["-PORT_WEIGHT-"] = 100 / len(strategies)
                window["-PORT_WEIGHT-"].update(
                    format_float(strategy_settings[strategy]["-PORT_WEIGHT-"])
                )

            # We must continue so the GUI does not update with old values from the values dict
            continue

        elif event == "-PORTFOLIO_MODE-":
            portfolio_mode = values["-PORTFOLIO_MODE-"]
            for key in [
                "-STRATEGY_SELECT-",
                "-PASSTHROUGH_MODE-",
                "-PORT_WEIGHT_TEXT1-",
                "-PORT_WEIGHT-",
                "-PORT_WEIGHT_TEXT2-",
            ]:
                window[key].update(visible=portfolio_mode)

            if portfolio_mode:
                files = values["-FILE-"].split(";")
                strategies = [os.path.basename(file) for file in files]
                window["-STRATEGY_SELECT-"].update(values=strategies)
                if strategies:
                    # select the first strategy in the list
                    window["-STRATEGY_SELECT-"].update(value=strategies[0])
            else:
                strategies = ["-SINGLE_MODE-"]

            # Initialize settings for each strategy
            strategy_settings.clear()
            for strategy in strategies:
                strategy_settings[strategy] = {}
                update_strategy_settings(values, strategy_settings[strategy])
                # set the portfolio weightings to equal weight
                strategy_settings[strategy]["-PORT_WEIGHT-"] = 100 / len(strategies)
                window["-PORT_WEIGHT-"].update(
                    format_float(strategy_settings[strategy]["-PORT_WEIGHT-"])
                )

        elif event == "-STRATEGY_SELECT-":
            selected_strategy = values["-STRATEGY_SELECT-"]
            if selected_strategy in strategy_settings:
                for key, value in strategy_settings[selected_strategy].items():
                    if key in window.AllKeysDict:
                        # print(f"updating key: {key} value: {value}")
                        window[key].update(format_float(value))

        elif event == "__TIMEOUT__":
            if chart_images:
                # Resize the image and update the element
                window_w, window_h = window.size
                image_width_max = int(window_w * 0.90)
                image_height_max = int(window_h * 0.40)
                image_width = min(
                    image_width_max, int(image_height_max / image_aspect_ratio)
                )
                image_size = (image_width, int(image_width * image_aspect_ratio))
                for chart, image_b64 in chart_images.items():
                    # we only need to pass the height
                    resized_image = resize_base64_image(image_b64, image_size[1])
                    window[chart].update(data=resized_image)

        elif event == "-THEME-":
            new_theme = themes[values["-THEME-"]]
            sg.theme(new_theme)
            sg.theme_button_color(button_color)  # override button color
            save_settings(app_settings, settings_filename, values)
            # Recreate the window with the new theme
            Checkbox.clear_elements()
            new_window = get_main_window(values.copy(), window)

            # Close the current window
            window.close()

            window = new_window
            continue

        elif event == "-CALC_TYPE-":
            window["-CALC_TYPE_TEXT-"].update(values["-CALC_TYPE-"])

        elif event == "Optimizer":
            save_settings(app_settings, settings_filename, values)
            # Validate settings for all strategies
            result = validate_strategy_settings(strategy_settings)
            if type(result) == str:
                # there was an error
                sg.popup_no_border(result)
                continue
            files_list = values["-FILE-"].split(";")
            if "" in files_list:
                files_list.remove("")
            for file in files_list:
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext != ".csv":
                    sg.popup_no_border(
                        "One or more of the selected files\ndo not appear to be a csv file!"
                    )
                    error = True
                    break
            if error:
                error = False  # reset
                continue
            if not files_list:
                sg.popup_no_border("Please Browse for a file first")
                continue
            optimizer_thread = optimizer_window(
                files_list, app_settings, strategy_settings
            )
            if optimizer_thread:
                window["-PROGRESS-"].update(visible=True)
                window["Analyze"].update("Working...", disabled=True)
                window["Cancel"].update(visible=True)
                test_running = True
        # Update strategy settings when values change but not while analysis is running
        if (
            values["-PORTFOLIO_MODE-"]
            and values["-STRATEGY_SELECT-"]
            and not test_running
        ):
            selected_strategy = values["-STRATEGY_SELECT-"]
            update_strategy_settings(values, strategy_settings[selected_strategy])
        elif not values["-PORTFOLIO_MODE-"] and not test_running:
            if "-SINGLE_MODE-" not in strategy_settings:
                strategy_settings["-SINGLE_MODE-"] = {}
            update_strategy_settings(values, strategy_settings["-SINGLE_MODE-"])

        # check if thread is done
        while True:
            if not results_queue.empty():
                result_key, results = results_queue.get(block=False)
            else:
                break

            if result_key == "-RUN_ANALYSIS_END-":
                run_analysis_process.join()
                if isinstance(results, Exception):
                    sg.popup_error(
                        f"Error during Analysis:\n{type(results).__name__}: {results}\nCheck log file for details.\n\nAre you sure this is a BYOB or OO csv?"
                    )
                else:
                    df_dicts = results
                    for right_type, day_dict in df_dicts.items():
                        for day, df_dict in day_dict.items():

                            top_times_df = get_top_times(df_dict, strategy_settings)
                            table_data = top_times_df.values.tolist()
                            if right_type.endswith("Gap Up"):
                                window[
                                    f"-TABLE_{right_type.removesuffix(" Gap Up")}_{"Gap Up"}_{day}-"
                                ].update(values=table_data, num_rows=len(table_data))
                            elif right_type.endswith("Gap Down"):
                                window[
                                    f"-TABLE_{right_type.removesuffix(" Gap Down")}_{"Gap Down"}_{day}-"
                                ].update(values=table_data, num_rows=len(table_data))
                            else:
                                window[f"-TABLE_{right_type}_{"All"}_{day}-"].update(
                                    values=table_data, num_rows=len(table_data)
                                )

                if values["-BACKTEST-"] and not isinstance(results, Exception):
                    path = os.path.join(
                        os.path.dirname(files_list[0]), "data", "trade_logs"
                    )
                    os.makedirs(path, exist_ok=True)
                    wf_test_process = Process(
                        target=walk_forward_test,
                        args=(
                            results_queue,
                            cancel_flag,
                            df_dicts,
                            path,
                            strategy_settings,
                        ),
                        kwargs={
                            "initial_value": float(values["-START_VALUE-"]),
                            "start": start_date,
                            "end": end_date,
                            "use_scaling": values["-SCALING-"],
                            "export_trades": values["-EXPORT-"],
                            "export_OO_sig": values["-EXPORT_OO_SIG-"],
                            "weekday_list": weekday_list,
                            "news_events": news_events,
                        },
                    )
                    wf_test_process.start()
                else:
                    window["-PROGRESS-"].update(visible=False)
                    window["Cancel"].update(visible=False)
                    window["Analyze"].update("Analyze", disabled=False)
                    test_running = False

            elif result_key == "-BACKTEST_END-":
                wf_test_process.join()
                window["-PROGRESS-"].update(visible=False)
                window["Cancel"].update(visible=False)
                window["Analyze"].update("Analyze", disabled=False)
                test_running = False
                if isinstance(results, Exception):
                    sg.popup_error(
                        f"Error during walk-forward test:\n{type(results).__name__}: {results}\nCheck log file for details."
                    )
                    continue
                check_result = True
                for result_df in results.values():
                    if result_df.empty:
                        check_result = False
                        break
                if not check_result:
                    sg.popup_no_border(
                        "One or more of your strategies or files contains no results.\nPerhaps the dataset does not go back far enough?"
                    )
                    continue
                table_data, img_data = get_pnl_plot(results)
                chart_images["-PNL_CHART-"] = img_data
                window["-PNL_TABLE_CHART-"].update(
                    values=table_data, num_rows=min(len(table_data), 4)
                )

                chart_images["-WEEKDAY_PNL_CHART-"] = get_weekday_pnl_chart(
                    results, weekday_list
                )
                chart_images["-MONTHLY_PNL_CHART-"] = get_monthly_pnl_chart(results)
                chart_images["-NEWS_PNL_CHART-"] = get_news_event_pnl_chart(
                    results, news_events
                )
                chart_images["-NEWS_AVG_PNL_CHART-"] = get_news_event_pnl_chart(
                    results, news_events, False
                )
                if values["-PORTFOLIO_MODE-"]:
                    chart_images["-CORRELATION_MATRIX-"] = get_correlation_matrix(
                        results
                    )
                    window["-CORRELATION_MATRIX_TAB-"].update(visible=True)
                # resize the images to fit in the window
                for chart, image_data in chart_images.items():
                    chart_image = resize_base64_image(
                        image_data,
                        int(window.size[1] * 0.2),
                    )
                    window[chart].update(data=chart_image)

                window["-TAB_GROUP-"].Widget.select(4)

                # recreate window to have table columns auto adjust
                new_theme = themes[values["-THEME-"]]
                sg.theme(new_theme)
                sg.theme_button_color(button_color)  # override button color

                # Recreate the window with the new theme
                Checkbox.clear_elements()
                new_window = get_main_window(values.copy(), window)

                # Close the current window
                window.close()

                window = new_window
                continue

            elif result_key == "-OPTIMIZER-":
                # handle error
                if isinstance(results, Exception):
                    sg.popup_error(
                        f"Error during Optimization:\n{type(results).__name__}: {results}\nCheck log file for details."
                    )
                    results_queue.put(("-BACKTEST_CANCELED-", "-OPTIMIZER-"))
                    continue

                optimizer_thread.join()
                best_performer, time_per_test, start_date = results
                best_performer: OptimizerResult = best_performer
                strat_name = best_performer.strat_name
                optimized_settings = best_performer.settings
                # update the settings in the window
                for key, value in optimized_settings.items():
                    if key in window.AllKeysDict:
                        window[key].update(format_float(value))
                    # update the values dict for later use
                    if key in values:
                        values[key] = value
                # set the end date for the later WF test
                end_date = None
                # update window with start date from optimizer
                window["-START_DATE-"].update(value=start_date.strftime("%Y-%m-%d"))

                # clear the current settings
                strategy_settings.clear()
                # set the new settings for single mode
                strategy_settings[strat_name] = optimized_settings

                # we will always run the result with port mode
                # this allows the options window settings to be applied
                window["-PORTFOLIO_MODE-"].update(value=True)
                values["-PORTFOLIO_MODE-"] = True
                for key in [
                    "-STRATEGY_SELECT-",
                    "-PASSTHROUGH_MODE-",
                    "-PORT_WEIGHT_TEXT1-",
                    "-PORT_WEIGHT-",
                    "-PORT_WEIGHT_TEXT2-",
                ]:
                    window[key].update(visible=True)
                window["-STRATEGY_SELECT-"].update(values=[strat_name])
                # select our optimized strategy
                window["-STRATEGY_SELECT-"].update(value=strat_name)
                values["-STRATEGY_SELECT-"] = strat_name
                # turn on backtest and scaling
                window["-BACKTEST-"].update(value=True)
                window["-SCALING-"].update(value=True)

                # get the complete file path
                file_list = values["-FILE-"].split(";")
                dir_name = os.path.dirname(file_list[0])
                file = os.path.join(dir_name, strat_name)
                logger.debug(f"File: {file}")

                logger.debug(strategy_settings)
                # run the analysis with the new settings
                run_analysis_process = Process(
                    target=run_analysis_threaded,
                    kwargs={
                        "files_list": [file],
                        "strategy_settings": strategy_settings,
                        "open_files": values["-OPEN_FILES-"],
                        "results_queue": results_queue,
                        "cancel_flag": cancel_flag,
                        "create_excel": True,
                        "news_events": news_events,
                    },
                )
                run_analysis_process.start()
                app_settings["-TIME_PER_TEST-"] = time_per_test
                save_settings(app_settings, settings_filename, values)
                break

            elif result_key == "-BACKTEST_CANCELED-":
                if results == "-RUN_ANALYSIS-":
                    run_analysis_process.join()
                elif results == "-WALK_FORWARD-":
                    wf_test_process.join()
                elif results == "-OPTIMIZER-":
                    optimizer_thread.join()
                window["-PROGRESS-"].update(visible=False)
                window["Cancel"].update("Cancel", disabled=False, visible=False)
                window["Analyze"].update("Analyze", disabled=False)
                test_running = False

            elif result_key == "-IMPORT_NEWS-":
                if isinstance(results, str):
                    if results.startswith("News"):
                        news_events_loaded = True
                    find_news_process.join()
                    sg.popup_no_border(results, auto_close=True, auto_close_duration=5)
                else:
                    news_events = results

            elif result_key == "-ERROR-":
                sg.popup_no_border(results)

        # move the progress bar
        if window["Analyze"].Disabled:
            window["-PROGRESS-"].Widget["value"] += 10
        else:
            window["-PROGRESS-"].Widget["value"] = 0

    window.close()


if __name__ == "__main__":
    freeze_support()
    main()
