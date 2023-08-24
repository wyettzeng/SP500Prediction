import pandas
import json
import requests


class FRED:
    def __init__(self):
        self.api_key = "c57e2f46f9672ed4f2e650596d7262e0"

    def getSeriesObservation(
        self, series_id: str, name: str = "data"
    ) -> pandas.DataFrame:
        """Retrieve a Data Observation from St. Louis Fred. Return a dataframe. Arguments:

        series_id: a string that can be found on St. Louis Fred to identify the data series, ex: "DGS5"
        name: the name that you would like to call this data, ex: "10 year yield" Leave this blank will name this data
        """
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={self.api_key}&file_type=json"
        response = requests.request("GET", url, headers={}, data={})

        dic = json.loads(response.text)["observations"]
        df = pandas.DataFrame(dic)[["date", "value"]]
        df = df.drop(df[df["value"] == "."].index)
        df["date"] = pandas.to_datetime(df["date"])
        df["value"] = pandas.to_numeric(df["value"])
        df = df.rename({"date": "Date", "value": name}, axis=1)
        df = df.set_index("Date")
        df = df.sort_index(ascending=False)
        return df

    def getMultipleSeriesObservation(self, input: dict) -> pandas.DataFrame:
        """call the getSeriesObservation multiple times and combine the output. Argument:

        input: a dictionary of (given name, series id), ex: {"U.S. Treasury Securities at 5-Year Constant Maturity", "DGS5"}
        """
        lst = []
        for name in input:
            lst.append(self.getSeriesObservation(input[name], name))
        if len(lst) == 0:
            raise Exception(f"invalid input {input}")
        if len(lst) == 1:
            return lst[0]
        else:
            df = lst[0].join(lst[1:], how="outer")
            df = df.sort_index(ascending=False)
            print(f"Asset with least amount of data: {df.count().idxmin()}")
            return df
