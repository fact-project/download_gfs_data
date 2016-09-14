import pygrib
import numpy as np
import requests
import logging
import pandas as pd
from collections import deque
from threading import Thread
from os.path import join, exists
import os
from dateutil.parser import parse as parse_date
import argparse
import tempfile


url = 'http://nomads.ncdc.noaa.gov/data/gfsanl/{date:%Y%m}/' \
    '{date:%Y%m%d}/gfsanl_3_{date:%Y%m%d}_{date:%H%M}_000.grb'

column_names = {
    'rel_hum':     'Relative humidity',
    'temperature': 'Temperature',
    'height':      'Geopotential Height',
}

parser = argparse.ArgumentParser()
parser.add_argument('start_date', help='First date to download data from')
parser.add_argument('end_date', help='Last date to download data from')
parser.add_argument('outputpath', help='Directory to store the data files')
parser.add_argument(
    '-n', '--num-threads',
    dest='num_threads', help='How many threads to use',
    default=4, type=int
)


def download_grb_file(date, outfile):
    cur_url = url.format(date=date)
    response = requests.get(
        cur_url,
        stream=True,
    )

    if not response.ok:
        message = 'Failed to download gfs data for date {}'.format(date)
        raise IOError(message)

    logging.info('Start download of {:%Y-%m-%d %H:%M}'.format(date))
    with open(outfile, 'wb') as f:
        for block in response.iter_content(1024):
            f.write(block)
    logging.info('Finished: {:%Y-%m-%d %H:%M}'.format(date))


def get_key(key, filename):
    with pygrib.open(filename) as data:
        lats, longs = data[1].latlons()
        longs = longs - 180
        mask = (lats <= 29) & (27 <= lats) & \
               (longs >= -19) & (longs <= -17)
        temp_msgs = data.select(name=key)

        levels = []
        temps = []
        for msg in temp_msgs:
            if msg['levelType'] == 'pl':
                levels.append(msg['level'])
                temps.append(msg.values[mask])

    return np.array(levels), np.mean(temps, axis=1), np.std(temps, axis=1)


def extract_data(date, filename, outputpath):
    for key, col in column_names.items():
        p, val_mean, val_std = get_key(col, filename)

        df = pd.DataFrame({
            'pressure': p,
            key + '_mean': val_mean,
            key + '_std': val_std,
        })
        df.to_csv(
            join(outputpath, '{:%Y-%m-%d_%H-%M}_{}.csv'.format(date, key)),
            index=False,
        )


class Worker(Thread):
    def __init__(self, queue, _id, outputpath):
        self.queue = queue
        self.outputpath = outputpath
        self._id = _id
        super().__init__()

    def run(self):
        while self.queue:
            date = self.queue.popleft()
            temp = tempfile.NamedTemporaryFile()
            try:
                download_grb_file(date, temp.name)
            except IOError:
                logging.exception('Could not download file')
            try:
                extract_data(
                    date, temp.name, self.outputpath
                )
            except:
                logging.exception('Could not parse')


def main():
    args = parser.parse_args()
    outputpath = args.outputpath
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    if not exists(outputpath):
        os.makedirs(outputpath)
        logger.info('Created dir {}'.format(outputpath))

    dates = pd.date_range(
        start=start_date,
        end=end_date,
        freq='6h',
    )
    queue = deque(dates)

    workers = [Worker(queue, i, outputpath) for i in range(args.num_threads)]

    try:
        for worker in workers:
            worker.start()

    except (KeyboardInterrupt, SystemExit):
        queue.clear()
    finally:
        for worker in workers:
            worker.join()


if __name__ == '__main__':
    main()
