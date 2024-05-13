from enum import Enum
import pandas as pd
import datetime as dt
class TestOutcome(Enum):
    PASSED = 1
    FAILED = 0
    ERROR = -1


class TestResult:
    def __init__(self, result: TestOutcome, dataset_name: str, test_name: str, message: str, value):
        self.result = result
        self.dataset_name = dataset_name
        self.test_name = test_name
        self.message = message
        self.value = value


class DataQualityTester:
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def __init__(self):
        self.results = []

    def pretty_print_results(self):
        for r in self.results:
            if r.result == TestOutcome.PASSED:
                print(f"{self.OKGREEN}PASSED{self.ENDC} - {r.dataset_name} - {r.test_name} - {r.message}")
            elif r.result == TestOutcome.FAILED:
                print(f"{self.FAIL}FAILED{self.ENDC} - {r.dataset_name} - {r.test_name} - {r.message}")
            else:
                print(f"{self.WARNING}ERROR{self.ENDC} - {r.dataset_name} - {r.test_name} - {r.message}")

    def results_to_df(self) -> pd.DataFrame:
        return pd.DataFrame([{"dataset": r.dataset_name, "test": r.test_name, "result": r.result.name,
                              "message": r.message, "value": r.value} for r in self.results])

    def results_to_csv(self, path: str):
        self.results_to_df().to_csv(path, index=False)

    def test_unique_col(self, df: pd.DataFrame, col: str, dataset_name: str):
        res = None
        try:
            count_of_non_unique = len(df) - len(df[col].unique())
            outcome = TestOutcome.FAILED if count_of_non_unique > 0 else TestOutcome.PASSED
            msg = f"Found {count_of_non_unique} non-unique values in column {col}"
            res = TestResult(outcome, dataset_name, "unique_col", msg, count_of_non_unique)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, dataset_name, "unique_col", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_duplicates_rows(self, df: pd.DataFrame, dataset_name: str):
        res = None
        try:
            count_of_duplicates = len(df) - len(df.drop_duplicates())
            outcome = TestOutcome.FAILED if count_of_duplicates > 0 else TestOutcome.PASSED
            msg = f"Found {count_of_duplicates} duplicates"
            res = TestResult(outcome, dataset_name, "duplicates", msg, count_of_duplicates)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, dataset_name, "duplicates", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_for_negative_values(self, df: pd.DataFrame, column: str, dataset_name: str) -> None:
        res = None
        try:
            count_of_negatives = len(df[df[column] < 0])
            outcome = TestOutcome.FAILED if count_of_negatives > 0 else TestOutcome.PASSED
            msg = f"Found {count_of_negatives} negative values in column {column}"
            res = TestResult(outcome, dataset_name, "negative_values", msg, count_of_negatives)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, "Raw Breadcrumbs", "negative_values", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_for_date_equality_raw(self, df: pd.DataFrame):
        res = None
        try:
            count = len(df["OPD_DATE"].unique())
            outcome = TestOutcome.FAILED if count > 1 else TestOutcome.PASSED
            msg = f"Found {count} unique dates"
            res = TestResult(outcome, "Raw Breadcrumbs", "date_equality", msg, count)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, "Raw Breadcrumbs", "date_equality", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_for_malformed_dates(self, df: pd.DataFrame):
        res = None

        def try_parse_date(date_str: str) -> bool:
            try:
                dt.datetime.strptime(date_str, "%d%b%Y:%H:%M:%S")
                return True
            except ValueError:
                return False

        try:
            count = len(df[~df["OPD_DATE"].apply(try_parse_date)])
            outcome = TestOutcome.FAILED if count > 0 else TestOutcome.PASSED
            msg = f"Found {count} malformed dates"
            res = TestResult(outcome, "Raw Breadcrumbs", "malformed_dates", msg, count)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, "Raw Breadcrumbs", "malformed_dates", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_value_above_threshold(self, df: pd.DataFrame, column: str, threshold: int, dataset_name: str) -> None:
        res = None
        try:
            count_of_values_below_threshold = len(df[df[column] >= threshold])
            outcome = TestOutcome.FAILED if count_of_values_below_threshold > 0 else TestOutcome.PASSED
            msg = f"Found {count_of_values_below_threshold} values above threshold {threshold} in column {column}"
            res = TestResult(outcome, dataset_name, "values_above_threshold", msg, count_of_values_below_threshold)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, dataset_name, "values_above_threshold", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_for_percentage_difference(self, df: pd.DataFrame, column: str, max_pct: float, dataset_name: str) -> None:
        res = None
        try:
            percentage_diff = (df[column].pct_change(fill_method=None) * 100).dropna()
            outcome = TestOutcome.FAILED if percentage_diff.abs().max() > max_pct else TestOutcome.PASSED
            msg = f"Found max percentage difference of {round(percentage_diff.abs().max(), 2)}% in column {column} E.g. {df.iloc[percentage_diff.abs().idxmax() - 1][column]} to {df.iloc[percentage_diff.abs().idxmax()][column]}"
            res = TestResult(outcome, dataset_name, "percentage_difference", msg, percentage_diff.abs().max())
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, dataset_name, "percentage_difference", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

    def test_for_missing_values(self, df: pd.DataFrame, column: str, dataset_name: str) -> None:
        res = None
        try:
            count_of_missing_values = len(df[df[column].isnull()])
            outcome = TestOutcome.FAILED if count_of_missing_values > 0 else TestOutcome.PASSED
            msg = f"Found {count_of_missing_values} missing values in column {column}"
            res = TestResult(outcome, dataset_name, "missing_values", msg, count_of_missing_values)
        except Exception as e:
            res = TestResult(TestOutcome.ERROR, dataset_name, "missing_values", str(e), None)
        finally:
            if res is not None:
                self.results.append(res)

