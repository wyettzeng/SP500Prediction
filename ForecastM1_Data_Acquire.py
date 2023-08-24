# This algorithm is developed by X. Zhong, D. Enke, but written by Wyett. Please do not use this as I probably make fatal mistake somewhere in my code.
# If you really want to use this please don't make any financial decision from this

import Yahoo
from StLouisFred import FRED
import datetime

fred = FRED()


def getSP500Data():
    df = Yahoo.getDailyData("SPY")
    df["y_up"] = (df["Close"] > df["Open"]).astype(int)
    df["y_down"] = (df["Close"] < df["Open"]).astype(int)
    df = df.rename(columns={"Close": "Close_SPY", "Volume": "V"})
    df["SPYt"] = df["Close_SPY"].pct_change(-1)  # daily return
    df["SPYt1"] = df["SPYt"].shift(-1)  # return of previous day
    df["SPYt2"] = df["SPYt"].shift(-2)  # return of 2 days ago
    df["SPYt3"] = df["SPYt"].shift(-3)  # return of 3 days ago
    df["RDP5"] = df["Close_SPY"].pct_change(-5)  # The 5-day % difference in SPY
    df["RDP10"] = df["Close_SPY"].pct_change(-10)  # The 10-day % difference in SPY
    df["RDP15"] = df["Close_SPY"].pct_change(-15)  # The 15-day % difference in SPY
    df["RDP20"] = df["Close_SPY"].pct_change(-20)  # The 20-day % difference in SPY
    df["EMA10"] = (
        df["Close_SPY"].sort_index(ascending=True).ewm(span=10, adjust=False).mean()
    )  # SPY exponential moving average
    df["EMA20"] = (
        df["Close_SPY"].sort_index(ascending=True).ewm(span=20, adjust=False).mean()
    )
    df["EMA50"] = (
        df["Close_SPY"].sort_index(ascending=True).ewm(span=50, adjust=False).mean()
    )
    df["EMA200"] = (
        df["Close_SPY"].sort_index(ascending=True).ewm(span=200, adjust=False).mean()
    )
    df = df.drop(
        ["Open", "High", "Low", "Dividends", "Stock Splits", "Capital Gains"], axis=1
    )
    return df.dropna()


def getStockETFData():
    df = Yahoo.getReturnData(
        {
            "HSI": "^HSI",
            "SSE Composite": "000001.SS",
            "FCHI": "^FCHI",
            "FTSE": "^FTSE",
            "GDAXI": "^GDAXI",
            "DJI": "^DJI",
            "IXIC": "^IXIC",
            "AAPL": "AAPL",
            "MSFT": "MSFT",
            "XOM": "XOM",
            "GE": "GE",
            "JNJ": "JNJ",
            "WFC": "WFC",
            "AMZN": "AMZN",
            "JPM": "JPM",
        }
    )

    return df


def getMarketData():
    """Macro economic variables such as spreads"""
    df = fred.getMultipleSeriesObservation(
        {
            "T1": "DGS1MO",
            "T3": "DGS3MO",
            "T6": "DGS6MO",
            "T60": "DGS5",
            "T120": "DGS10",
            "AAA_val": "DAAA",
            "BAA_val": "DBAA",
            "T12": "DGS1",
            "USDJPY": "DEXJPUS",
            "USDGBP": "DEXUSUK",
            "USDCAD": "DEXCAUS",
            "USDCNY": "DEXCHUS",
        }
    )  # getting market yields and fx rates

    # reversing us uk fx rate
    df["USDGBP"] = 1 / df["USDGBP"]

    df["AAA"] = df["AAA_val"].diff(-1)  # change in market yield of selected asset
    df["BAA"] = df["BAA_val"].diff(-1)
    df["CBT3M"] = df["T3"].diff(-1)
    df["CBT6M"] = df["T6"].diff(-1)
    df["CBT1Y"] = df["T12"].diff(-1)
    df["CBT5Y"] = df["T60"].diff(-1)
    df["CBT10Y"] = df["T120"].diff(-1)

    df["TE1"] = df["T120"] - df["T1"]  # term spreads
    df["TE2"] = df["T120"] - df["T3"]
    df["TE3"] = df["T120"] - df["T6"]
    df["TE5"] = df["T3"] - df["T1"]
    df["TE6"] = df["T6"] - df["T1"]

    df["DE1"] = df["BAA_val"] - df["AAA_val"]  # default spread
    df["DE2"] = df["BAA_val"] - df["T120"]
    df["DE4"] = df["BAA_val"] - df["T6"]
    df["DE5"] = df["BAA_val"] - df["T3"]
    df["DE6"] = df["BAA_val"] - df["T1"]

    df["USD_Y"] = df["USDJPY"].diff(-1)  # change in fx rate
    df["USD_GBP"] = df["USDGBP"].diff(-1)
    df["USD_CAD"] = df["USDCAD"].diff(-1)
    df["USD_CNY"] = df["USDCNY"].diff(-1)

    df = df.drop(
        ["AAA_val", "BAA_val", "T12", "USDJPY", "USDGBP", "USDCAD", "USDCNY"],
        axis=1,
    )

    return df.dropna()


def getCommoditiesData():
    df = Yahoo.getDailyPrices(
        {
            "Gold": "GLD",
            "Oil": "CL=F",
        }
    )
    df["Gold"] = df["Gold"].diff(-1)
    df["Oil"] = df["Oil"].diff(-1)
    return df.dropna()


def getData():
    lst = []
    lst.append(getStockETFData())
    lst.append(getCommoditiesData())
    lst.append(getMarketData())
    df = getSP500Data()
    df = df.join(lst, how="outer")
    df = df[df.index >= datetime.datetime(2005, 1, 1)]
    df["Date_SPY"] = (df.index - datetime.datetime(2005, 1, 1)).days
    df = df.reset_index(drop=True)
    df = df.dropna()

    q1 = df.quantile(0.25, axis=0)
    q3 = df.quantile(0.75, axis=0)
    iqr = q3 - q1
    for i in df.columns.values:
        df[i] = df[i].apply(
            lambda x: q1[i] - 1.5 * iqr[i]
            if x < q1[i] - 1.5 * iqr[i]
            else q3[i] + 1.5 * iqr[i]
            if x > q3[i] + 1.5 * iqr[i]
            else x
        )
    return df


df = getData()
print(df)
df.to_excel("Input data.xlsx", index=False)
