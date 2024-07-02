from sheep import *
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

def get_velocities(rel, projected_columns='*'):
    window = "OVER (PARTITION BY trajectory_id ORDER BY t)"
    return rel.project(f'''
        {projected_columns},
        ST_Distance_Spheroid(pos, lag(pos) {window}) /
            epoch(age(t, lag(t) {window}))
        AS velocity,
    ''')

def get_time_of_day(rel, projected_columns='*'):
    t = "2 * pi() * epoch(age(t, datetrunc('day', t))) / (24*3600)"
    return rel.project(f'''
        {projected_columns},
        cos({t}) AS tx,
        sin({t}) AS ty
    ''')

def cluster_elbow(data):
    y_ = np.array([
        data['velocity'],
        data['tx'],
        data['ty']
    ]).T
    x = np.arange(2, 10)
    y = []
    for k in x:
        print(k)
        kmeans = KMeans(n_clusters=k).fit(y_)
        y.append(kmeans.inertia_)
    plt.plot(x, y)
    plt.ylabel("inertia")
    plt.xlabel("number of clusters")
    #plt.axvline(4, linestyle='--', color='k')
    plt.show()

def cluster(rel, k):
    data = rel.fetchnumpy()
    y = np.array([
        data['velocity'],
        data['tx'],
        data['ty']
    ]).T
    clusters = KMeans(n_clusters=k).fit_predict(y)
    return db.query('''
        SELECT
            column0,
            avg(velocity) AS velocity,
            to_seconds(24*3600 * atan2(avg(ty), avg(tx)) / (2 * pi())) AS timeofday,
            sqrt(pow(avg(tx), 2) + pow(avg(ty), 2)) AS t_amplitude
            FROM prepare_cluster POSITIONAL JOIN clusters
            GROUP BY ROLLUP (column0)
            ORDER BY column0
    ''')

def prepare_cluster():
    rel = get_positions()
    rel = filter_season(rel)
    rel = remove_nullpos(rel)
    rel = remove_duplicates(rel)
    rel = identify_trajectories(rel)
    rel = get_time_of_day(rel)
    rel = rel.project('''
        *,
        extract('month' FROM t) AS month,
        extract('year' FROM t) AS year
    ''')
    rel.create("prepare_cluster")
    return db.table("prepare_cluster")
