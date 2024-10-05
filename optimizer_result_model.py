class OptimizerResult:
    def __init__(
        self,
        strat_name: str = None,
        settings: dict = None,
        selection_metric: str = None,
        lineage=None,
    ) -> None:
        self.lineage = lineage
        self.strat_name = strat_name
        self.settings = settings

        self.run_analysis_kwargs = None
        self.wf_test_kwargs = None

        self.run_analysis_result = None
        self.wf_result = None

        self.selection_metric = selection_metric

    def __str__(self) -> str:
        prefix = ""
        metrics_str = ""
        for attr in [
            "MAR",
            "Sharpe",
            "Total Return",
            "CAGR",
            "Drawdown%",
            "Days in Drawdown",
            "Largest Month",
            "Smallest Month",
        ]:
            if hasattr(self, attr):
                if attr == "MAR":
                    metric_str = f"{attr}: {getattr(self, attr):.3f}"
                elif attr == "Sharpe":
                    metric_str = f"{attr}: {getattr(self, attr):.3f}"
                elif attr == "Total Return":
                    metric_str = f"{attr}: {getattr(self, attr):.2%}"
                elif attr == "CAGR":
                    metric_str = f"{attr}: {getattr(self, attr):.2%}"
                elif attr == "Drawdown%":
                    metric_str = f"{attr}: {getattr(self, attr):.2%}"
                elif attr == "Days in Drawdown":
                    metric_str = f"{attr}: {getattr(self, attr)}"
                elif attr == "Largest Month":
                    metric_str = f"{attr}: {getattr(self, attr):.2f}"
                elif attr == "Smallest Month":
                    metric_str = f"{attr}: {getattr(self, attr):.2f}"
                if attr == self.selection_metric:
                    prefix += metric_str
                else:
                    metrics_str += f" | {metric_str}"
        metrics_str = prefix + metrics_str
        return (
            f"[Avg1: {self.settings['-AVG_PERIOD_1-']}mo({self.settings['-PERIOD_1_WEIGHT-']}%) | "
            f"Avg2: {self.settings['-AVG_PERIOD_2-']}mo({self.settings['-PERIOD_2_WEIGHT-']}%) | "
            f"Tranches: {self.settings['-TOP_X-']} | "
            f"Calc Type: {self.settings['-CALC_TYPE-']} | "
            f"Agg: {self.settings['-AGG_TYPE-']} | "
            f"Best P or C: {self.settings['-PUT_OR_CALL-']} | "
            f"Weekday: {self.settings['-IDV_WEEKDAY-']}]\n"
            f"{metrics_str}"
        )
