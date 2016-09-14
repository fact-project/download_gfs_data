import pandas as pd
from datetime import datetime
from os.path import join, split
from glob import glob
import numpy as np
from tqdm import tqdm


key = 'temperature'
indir = '/fhgfs/groups/app/fact/gfs_weather_data'
infiles = glob(join(indir, '*_{}.csv'.format(key)))

p_min = 800


def effective_temperature(T, P, Lambda_pi=16, Lambda_N=12):
    X = P / 9.81
    sum1 = np.sum(T / X * (np.exp(-X / Lambda_pi) - np.exp(-X / Lambda_N)))
    sum2 = np.sum(1 / X * (np.exp(-X / Lambda_pi) - np.exp(-X / Lambda_N)))
    return sum1 / sum2


keys = None

df = pd.DataFrame()

for infile in tqdm(infiles):
    directory, f = split(infile)
    date = datetime.strptime(
        f,
        '%Y-%m-%d_%H-%M_{}.csv'.format(key),
    )

    data = pd.read_csv(
        infile,
    )
    data = data.query('pressure >= {}'.format(p_min))

    d = {'date': date}

    d['effective_temperature'] = effective_temperature(
        data.temperature_mean, data.pressure * 100
    )

    new = pd.Series(d)
    df = df.append(new, ignore_index=True)

df.to_csv('gfs_effective_temperature.csv', index=False)
