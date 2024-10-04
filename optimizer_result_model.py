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
        metrics_str = f"{self.selection_metric}: {getattr(self, self.selection_metric)}"
        for attr in [
            "Max DD",
            "DD Days",
            "Total Return",
            "CAGR",
            "Sharpe",
            "MAR",
            "Largest Month",
            "Smallest Month",
        ]:
            if attr != self.selection_metric and hasattr(self, attr):
                metrics_str += f" | {attr}: {getattr(self, attr)}"
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
