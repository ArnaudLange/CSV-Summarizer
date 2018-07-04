import pandas as pd
import numpy as np
import sys
from dateutil.parser import parse

files = sys.argv[1:]


def isDate(string):
    try:
        parse(str(string))
        return True
    except ValueError:
        return False


def isNumber(s):
    try:
        return not np.isnan(float(s))
    except ValueError:
        pass
    return False


for file in files:
    df = pd.read_csv(file, sep=';', header=None)

    for column in df:
        currentCol = df[column]
        print("-------------")
        print("Column: " + str(column))

        nbValues = len(currentCol)
        nbFilledValues = currentCol.count()
        print("Allo: " + str(nbFilledValues))

        fillRate = int(np.round((nbFilledValues * 100) / nbValues))

        distinctValues = currentCol.nunique(dropna=False)

        numberRate = int(np.round((currentCol.apply(
            lambda s: (isNumber(s))).sum() * 100) / nbValues))
        numberFilledRate = int(np.round((currentCol.apply(
            lambda s: (isNumber(s))).sum() * 100) / nbFilledValues))

        dateRate = int(np.round((currentCol.astype(dtype='str').apply(
            lambda s: (isDate(s))).sum() * 100) / nbValues))
        dateFilledRate = int(np.round((currentCol.astype(dtype='str').apply(
            lambda s: (isDate(s))).sum() * 100) / nbFilledValues))

        print("Probable name: " + str(currentCol[0]))

        print("Number of values: " + str(nbValues))

        print("Number of filled values: " + str(nbFilledValues))
        print("Filling rate: " + str(fillRate) + "%")

        print("Number of distinct values: " + str(distinctValues))

        print("% of valid numbers: " + str(numberRate) + "%")
        print("% of valid numbers (among filled values): " +
              str(numberFilledRate) + "%")

        print("% of valid dates: " + str(dateRate) + "%")
        print("% of valid dates (among filled values): " +
              str(dateFilledRate) + "%")
