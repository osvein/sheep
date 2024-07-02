import duckdb
import numpy as np
from matplotlib import ticker
import matplotlib.pyplot as plt
from scipy.signal import find_peaks
from scipy.stats import gaussian_kde
from sklearn.neighbors import KernelDensity
from seaborn import heatmap
from sheep import *

def get_dt_array(rel):
    rel = rel.project("epoch(age(t, lag(t) OVER (PARTITION BY trajectory_id ORDER BY t)))")
    return tuple(rel.fetchnumpy().values())[0]

def plot_dt_density(ax, dt, bandwidth=0.2, max_hours=48):
    # division by zero caused by numpy bug: https://github.com/numpy/numpy/issues/4959
    f = np.log2(dt / (max_hours * 3600))
    kde = gaussian_kde(f, bw_method=bandwidth)
    x = np.linspace(-20, 0, 500)
    y = kde(x)
    ax.set_xlabel(f"Interval (/{max_hours} hours)")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x,_: f"$2^{{{int(x)}}}$"))
    ax.plot(x, y)
    peaks, _ = find_peaks(y)
    for xpeak in x[peaks]:
        ax.axvline(xpeak, color='k', linestyle='--')
    return x[peaks], y[peaks]

def get_predation_stats():
    df = db.query('''
        FROM (
            SELECT
                UTRANGERINGSAARSAK_ID AS type,
                extract('year' FROM UTDATO) AS year
            FROM Animal
            WHERE
                UTRANGERINGSAARSAK_ID BETWEEN 22 AND 29
        ) PIVOT (
            count()
            FOR year IN (2015,2016,2017,2018,2019,2020,2021,2022,2023)
            GROUP BY type
        )
    ''').fetchdf().set_index("type")
    labels = ["wolf", "bear", "lynx", "wolverine", "eagle", "fox", "dog", "unknown"]
    df = df.set_index(df.index.map(lambda x: labels[x - 22]))
    df.loc["total"] = df.sum()
    df.loc[:, "total"] = df.sum(axis=1)
    heatmap(df, annot=True, fmt='d')
    plt.show()
