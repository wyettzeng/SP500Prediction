# this class contains helper functions that interact with

import yfinance as yf
import pandas
import os
import re
import datetime
import pinyin


def goodFileName(s: str):
    return re.sub("[^0-9a-zA-Z \-]+", " ", pinyin.get(s, format="strip", delimiter=""))


def dailyDataHelper(df: pandas.DataFrame):
    df.index = df.index.date
    df.index = pandas.to_datetime(df.index)
    df = df.reset_index()
    df = df.rename({"index": "Date"}, axis="columns")
    df = df.dropna(subset=["Close"])
    df = df.sort_values("Date", ascending=False)
    df = df.reset_index(drop=True)
    return df


def updateAssetDailyData(ticker: str):
    """Use this function to update the csv file in the data folder to reflect the most recent daily data, including
    open, close, high, low prices, volume, dividends, stock splits"""
    fileName = f"data/daily data - {goodFileName(ticker)}.csv"
    if os.path.exists(fileName):
        df = pandas.read_csv(fileName)
        df["Date"] = pandas.to_datetime(df["Date"])
        latest = max(df["Date"])
        if (
            latest.date()
            < (datetime.datetime.now() - datetime.timedelta(days=1)).date()
        ):
            stock = yf.Ticker(ticker)
            df2 = stock.history(
                start=(latest + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            )
            if len(df2) == 0:
                return
            df2 = dailyDataHelper(df2)
            if len(df2) == 0:
                # no more available data
                return
            df3 = pandas.concat([df2, df], ignore_index=True)
            df3 = df3.drop_duplicates(subset="Date", keep="first")
            df3.sort_values("Date", ascending=False, inplace=True)
            df3.to_csv(fileName, index=False)
    else:
        stock = yf.Ticker(ticker)
        df = stock.history(period="max")
        if len(df) == 0:
            raise Exception("No data provided " + ticker)
        df = dailyDataHelper(df)
        if len(df) > 0:
            df.to_csv(fileName, index=False)
        else:
            raise Exception(f"No data return for ticker: {ticker}")


def getDailyData(ticker: str):
    """Use this funciton to get the daily data of 1 specific asset by providing its ticker"""
    updateAssetDailyData(ticker)
    fileName = f"data/daily data - {goodFileName(ticker)}.csv"
    df = pandas.read_csv(fileName)
    df.set_index("Date", inplace=True)
    df.index = pandas.to_datetime(df.index)
    return df


def getDailyPrice(ticker: str):
    """Use this funciton to get the daily price of 1 specific asset by providing its ticker"""
    updateAssetDailyData(ticker)
    df = getDailyData(ticker)
    df.rename({"Close": ticker}, axis="columns", inplace=True)
    return df[ticker].to_frame()


def getDailyDividend(ticker: str):
    """Return the daily dividend"""
    updateAssetDailyData(ticker)
    df = getDailyData(ticker)
    df.rename({"Dividends": ticker}, axis="columns", inplace=True)
    return df[ticker].to_frame()


def weightingToHoldingNbr(weighting: dict, investmentAmount: float = 100):
    """Given the weighting of a portfolio, finds out the number of holding for each asset required

    weighting - a dictionary with key being the ticker, and value being its weight. Ex: {"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}
    investmentAmount - how much you want the initial investment amount to be, ex: 100$
    """
    dic = {}
    for i in weighting:
        dic[i] = i
    df = getDailyPrices(dic)
    df.dropna(inplace=True)
    df.sort_index(ascending=True, inplace=True)
    nbrHoldings = {}
    initialPrice = df.iloc[0]
    for i in weighting:
        nbrHoldings[i] = investmentAmount * weighting[i] / initialPrice[i]
    return nbrHoldings


def getDailyPriceForCompositePortfolio(
    weighting: dict, portfolioName: str = "Portfolio 1"
):
    """This function gets the daily price of a composite portfolio. The starting price will be 100 (arbitrarily chosen)

    Argumets:
    weighting - a dictionary with key being the ticker, and value being its weight. Ex: {"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}
    portfolioName - the name of this composite portfolio, default is 'Portfolio 1'
    """
    dic = {}
    for i in weighting:
        dic[i] = i
    df = getDailyPrices(dic)
    df.dropna(inplace=True)
    df.sort_index(ascending=True, inplace=True)
    nbrHoldings = weightingToHoldingNbr(weighting=weighting)

    df = df.multiply(nbrHoldings)
    df = df.sum(axis=1)
    df.name = portfolioName
    return df.sort_index(ascending=False).to_frame()


def getDailyDividendForCompositePortfolio(
    weighting: dict, portfolioName: str = "Portfolio 1"
):
    """This function gets the daily price of a composite portfolio. The starting price will be 100 (arbitrarily chosen)

    Argumets:
    weighting - a dictionary with key being the ticker, and value being its weight. Ex: {"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}
    portfolioName - the name of this composite portfolio, default is 'Portfolio 1'
    """
    dic = {}
    for i in weighting:
        dic[i] = i
    df = getDailyDividends(dic)
    df.dropna(inplace=True)
    nbrHoldings = weightingToHoldingNbr(weighting=weighting)
    df = df.multiply(nbrHoldings)
    df = df.sum(axis=1)
    df.name = portfolioName
    return df.sort_index(ascending=False).to_frame()


def compositePortfolioHelper(dic: dict, tickerDataFunction, compositeDataFunction):
    """Arguments:

    dic : a dictionary like below:
        Key: Asset's Name
        Value: Asset's Ticker | for composite portfolio, a dictionary of (ticker, weighting)
        Example: dic = {"Microsoft": "MSFT", "Apple": "AAPL", 'Portfolio 1':{"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}}
    tickerDataFunction : a function to call for grabbing data of a ticker, ex: getDailyPrice
    compositeDataFunction : a function to cal for grabbing data of a composite portfolio, ex: getDailyPriceForCompositePortfolio
    """
    lst = list(
        map(
            lambda x: tickerDataFunction(dic[x])
            if type(dic[x]) == str
            else compositeDataFunction(dic[x], x),
            dic,
        )
    )
    if len(lst) == 0:
        return None
    if len(lst) == 1:
        return lst[0]
    else:
        df = lst[0].join(lst[1:], how="outer")
        df = df.sort_index(ascending=False).rename(
            dict((v, k) for k, v in dic.items() if type(v) == str), axis="columns"
        )
        print(f"Asset with least amount of data: {df.count().idxmin()}")
        return df.dropna()


def getDailyPrices(dic: dict):
    """Return a dataframe for the price of assets, the inputted dictionary should be in the following format:

    Key: Asset's Name
    Value: Asset's Ticker | for composite portfolio, a dictionary of (ticker, weighting)
    Example: dic = {"Microsoft": "MSFT", "Apple": "AAPL", 'Portfolio 1':{"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}}
    """

    return compositePortfolioHelper(
        dic, getDailyPrice, getDailyPriceForCompositePortfolio
    )


def getDailyDividends(dic: dict):
    """Return a dataframe for the price of assets, the inputted dictionary should be in the following format:

    Key: Asset's Name
    Value: Asset's Ticker | for composite portfolio, a dictionary of (ticker, weighting)
    Example: dic = {"Microsoft": "MSFT", "Apple": "AAPL", 'Portfolio 1':{"MSFT": 0.5, "AAPL": 0.4, "GOOGL": 0.1}}
    """
    return compositePortfolioHelper(
        dic, getDailyDividend, getDailyDividendForCompositePortfolio
    )


def getReturnData(input: dict | pandas.DataFrame):
    """Return a dataframe for the price return of assets, the inputted argument can be of 2 types:

    - a dictionary the following format:
        Key: Asset's Name
        Value: Asset's Ticker
        Example: dic = {"Microsoft": "MSFT", "Apple": "AAPL"}

    - a pandas dataframe with the prices of each asset"""

    if type(input) == dict:
        df = getDailyPrices(input)
        return df.sort_index(ascending=False).pct_change(-1).dropna()
    elif type(input) == pandas.DataFrame:
        return input.sort_index(ascending=False).pct_change(-1).dropna()
    else:
        raise Exception(
            f"Unsupported input type. Provided {type(input)} but only Pandas DataFrame or Dictionary is allowed"
        )


if not os.path.exists("data"):
    os.mkdir("data")


indicesTicker = {
    "S&P 500": "^GSPC",
    "Russell 2000": "^RUT",
    "S&P-TSX Composite": "^GSPTSE",
    "Nasdaq Composite": "^IXIC",
    "SSE Composite": "000001.SS",
    "MSCI World (ETD)": "MXWO.L",
    "Dow Jones": "^DJI",
}

exoticAssets = {
    "Gold": "GC=F",
    "Crude Oil": "CL=F",
    "Bit Coin": "BTC-USD",
}

equitiesTicker = {
    "Microsoft": "MSFT",
    "Apple": "AAPL",
    "Nvidia": "NVDA",
    "Visa": "V",
    "Mastercard": "MA",
    "JPMorgan": "JPM",
    "Bank of America": "BAC",
    "BlackRock": "BLK",
    "Google": "GOOGL",
    "Meta": "META",
    "Johnson & Johnson": "JNJ",
    "UnitedHealth Group": "UNH",
    "Amazon": "AMZN",
    "Tesla": "TSLA",
    "Walmart": "WMT",
    "Costco": "COST",
    "Coca Cola": "KO",
    "Pepsi": "PEP",
    "Exxon": "XOM",
}
