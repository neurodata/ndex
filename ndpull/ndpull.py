'''
Command line program to download a region of data from the BOSS as individual TIFF slices
'''

import argparse
import configparser
import json
import math
import sys
import time
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from collections import defaultdict

import blosc
import numpy as np
import pytest
import requests
import tifffile as tiff
from tqdm import tqdm

BOSS_VERSION = "v1"

# download blocks of size 2k by 2k by 16 (xyz), should be multiples of 512 x 512 x 16
CHUNK_SIZE = (2048, 2048, 16)


class BossMeta:
    def __init__(self, collection, experiment, channel):
        self._collection = collection
        self._experiment = experiment
        self._channel = channel

    def channel(self):
        return self._channel

    def experiment(self):
        return self._experiment

    def collection(self):
        return self._collection


class BossRemote:
    def __init__(self, boss_url, token, meta):
        self.boss_url = boss_url
        if self.boss_url[-1] != '/':
            self.boss_url += '/'
        self.token = token

        # BossMeta contains col, exp, chn info
        self.meta = meta

        self.session = requests.Session()
        self.session.headers = {'Authorization': 'Token {}'.format(self.token)}

    def __str__(self):
        string = 'Collection: {}, Experiment: {}, Channel: {}\n'.format(
            self.meta.collection(), self.meta.experiment(), self.meta.channel())

        indent_size = 2

        metadata = self.get_exp_metadata()
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Experiment metadata:\n{}\n\n'.format(metadata_str)

        metadata = self.get_coord_frame_metadata(exp_metadata=metadata)
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Coordinate frame metadata:\n{}\n\n'.format(metadata_str)

        metadata = self.get_channel_metdata()
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Channel metadata:\n{}\n\n'.format(metadata_str)

        return string

    def get(self, url, headers={}):
        if url[0] == '/':
            url = url[1:]
        r = self.session.get("{}{}".format(
            self.boss_url, url), headers=headers)
        return r

    def get_exp_metadata(self):
        # https://api.theboss.io/v1/collection/:collection/experiment/:experiment/
        exp_url = "{}/collection/{}/experiment/{}/".format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment()
        )
        resp = self.get(exp_url, {'Accept': 'application/json'})
        return resp.json()

    def get_channel_metdata(self):
        # https://api.boss.neurodata.io/v1/collection/:collection/experiment/:experiment/channel/:channel/
        ch_url = '{}/collection/{}/experiment/{}/channel/{}/'.format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment(), self.meta.channel()
        )
        resp = self.get(ch_url, {'Accept': 'application/json'})
        return resp.json()

    def get_coord_frame_metadata(self, exp_metadata=None):
        coord_frame_name = self.get_coord_frame_name(exp_metadata)
        coord_frame_url = "{}/coord/{}".format(BOSS_VERSION, coord_frame_name)
        resp = self.get(coord_frame_url, {'Accept': 'application/json'})
        return resp.json()

    def get_coord_frame_name(self, exp_data=None):
        if exp_data is None:
            exp_data = self.get_exp_metadata()
        return exp_data['coord_frame']

    def get_xyz_extents(self):
        coord_frame = self.get_coord_frame_metadata()
        x_rng = [coord_frame['x_start'], coord_frame['x_stop']]
        y_rng = [coord_frame['y_start'], coord_frame['y_stop']]
        z_rng = [coord_frame['z_start'], coord_frame['z_stop']]
        return x_rng, y_rng, z_rng

    def cutout(self, x_rng, y_rng, z_rng, datatype, res=0, attempts=10):
        cutout_url_base = "{}/cutout/{}/{}/{}".format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment(), self.meta.channel())
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, res, x_rng[0], x_rng[1], y_rng[0], y_rng[1], z_rng[0], z_rng[1])

        for attempt in range(attempts):
            try:
                resp = self.get(cutout_url, {'Accept': 'application/blosc'})
                resp.raise_for_status()
            except Exception:
                if attempt != attempts - 1:
                    time.sleep(2**(attempt + 1))
            else:
                break
        else:
            # we failed all the attempts - deal with the consequences.
            raise ConnectionError(
                'Data from URL {} not fetched.  Status code {}, error {}'.format(
                    cutout_url, resp.status_code, resp.reason))

        raw_data = blosc.decompress(resp.content)
        data = np.fromstring(raw_data, dtype=datatype)

        return np.reshape(data,
                          (z_rng[1] - z_rng[0],
                           y_rng[1] - y_rng[0],
                           x_rng[1] - x_rng[0]),
                          order='C')


def get_cube_lims(rng, stride=16):
    # stride = height of super cuboid

    first = rng[0]    # inclusive
    last = rng[1]     # exclusive

    buckets = defaultdict(list)
    for z in range(first, last):
        buckets[(z // stride)].append(z)

    return buckets


def collect_input_args(collection, experiment, channel, config_file=None, token=None, url='https://api.boss.neurodata.io', x=None, y=None, z=None, res=0, outdir='./', full_extent=False, print_metadata=False):
    result = argparse.Namespace(
        collection=collection,
        experiment=experiment,
        channel=channel,
        config_file=config_file,
        token=token,
        url=url,
        x=x, y=y, z=z, res=res, outdir=outdir,
        full_extent=full_extent,
        print_metadata=print_metadata
    )
    return result


def collect_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', type=str,
                        help='User config file for BOSS')
    parser.add_argument(
        '--token', type=str, help='User token for the boss (not used if config file specified)')
    parser.add_argument(
        '--url', default='https://api.boss.neurodata.io', help='URL to boss endpoint (not used if config file specified)')

    parser.add_argument('--collection', type=str, help='Collection')
    parser.add_argument('--experiment', type=str, help='Experiment')
    parser.add_argument('--channel', type=str, help='Channel')

    # parser.add_argument('--download_limit', type=int, default=500,
    #                     help='limit total download size to this value in GB')

    parser.add_argument('--x', nargs=2, type=int, help='X range for stack')
    parser.add_argument('--y', nargs=2, type=int, help='Y range for stack')
    parser.add_argument('--z', nargs=2, type=int, help='Z range for stack')
    parser.add_argument('--res', default=0, type=int, help='Stack resolution')

    parser.add_argument('--outdir', type=str, default="./",
                        help='Path to output directory.')

    parser.add_argument('--full_extent', action='store_true',
                        help='Use the full extent of the data on the BOSS')

    parser.add_argument('--print_metadata', action='store_true',
                        help='Prints the metadata on the collection/experiment/channel and quits')

    result = parser.parse_args()
    return result


def get_boss_config(boss_config_file):
    config = configparser.ConfigParser()
    config.read(boss_config_file)
    token = config['Default']['token']
    boss_url = ''.join(
        (config['Default']['protocol'], '://', config['Default']['host']))
    return token, boss_url


def download_slices(result, rmt, threads=4):

    # get the datatype
    ch_meta = rmt.get_channel_metdata()
    datatype = ch_meta['datatype']

    z_buckets = get_cube_lims(result.z, stride=CHUNK_SIZE[2])
    for _, z_slices in tqdm(z_buckets.items()):
        z_rng = [z_slices[0], z_slices[-1] + 1]

        # re-initialize on every slice of z
        # zyx ordered
        data_slices = np.zeros((z_rng[1] - z_rng[0],
                                result.y[1] - result.y[0],
                                result.x[1] - result.x[0]),
                               dtype=datatype)

        for y in range(result.y[0], result.y[1], CHUNK_SIZE[1]):
            y_rng = [y, result.y[1] if (
                y + CHUNK_SIZE[1]) > result.y[1] else (y + CHUNK_SIZE[1])]

            x_rngs = []
            for x in range(result.x[0], result.x[1], CHUNK_SIZE[0]):
                x_rngs.append([x, result.x[1] if (
                    x + CHUNK_SIZE[0]) > result.x[1] else (x + CHUNK_SIZE[0])])

            cutout_partial = partial(
                rmt.cutout, y_rng=y_rng, z_rng=z_rng, datatype=datatype, res=result.res)
            with ThreadPool(threads) as pool:
                data_list = pool.map(cutout_partial, x_rngs)

            # data = rmt.cutout(x_rng, y_rng, z_rng, datatype, result.res)
            for data, x_rng in zip(data_list, x_rngs):
                # insert into numpy array
                data_slices[:,
                            y_rng[0] - result.y[0]:y_rng[1] - result.y[0],
                            x_rng[0] - result.x[0]:x_rng[1] - result.x[0]] = data

        save_to_tiffs(data_slices, rmt.meta, result, z_rng)


def save_to_tiffs(data_slices, meta, result, z_rng):
    # save the numpy array as a tiff file

    # path for saving slices
    cutout_path = result.outdir
    if cutout_path[-1] != '/':
        cutout_path += '/'

    digits = int(math.log10(result.z[1])) + 1

    for zslice in range(z_rng[0], z_rng[1]):
        fname = '{}_{}_{}_x{x[0]}-{x[1]}_y{y[0]}-{y[1]}_z{z:0{dig}d}.tif'.format(
            meta.collection(), meta.experiment(), meta.channel(),
            x=result.x, y=result.y, z=zslice, dig=digits)

        data = data_slices[zslice - z_rng[0], :, :]
        tiff.imsave(cutout_path + fname, data,
                    metadata={'DocumentName': fname}, compress=6)


# to add:
# tracking of amount of data requests (with default limit, so it can stop if it passes a threshold)


def validate_args(result):
    # get tokens from config file if set as an option
    if result.config_file:
        result.token, result.url = get_boss_config(result.config_file)
    if result.token is None:
        error_msg = 'Need token or config file'
        print(error_msg)
        raise ValueError(error_msg)

    meta = BossMeta(result.collection, result.experiment, result.channel)
    rmt = BossRemote(result.url, result.token, meta)

    if result.print_metadata:
        print(rmt)
        sys.exit()

    # list of full extent in xyz
    full_range = rmt.get_xyz_extents()

    if result.full_extent:
        if any([a is not None for a in [result.x, result.y, result.z]]):
            error_msg = 'full extent not compatible with specified x, y, or z ranges'
            print(error_msg)
            raise ValueError(error_msg)
        result.x, result.y, result.z = full_range
    else:
        if any([a is None for a in [result.x, result.y, result.z]]):
            error_msg = 'Need extents for x, y, and z for download'
            print(error_msg)
            raise ValueError(error_msg)
        if not all([a[0] >= b[0] and a[1] <= b[1]
                    for a, b in zip([result.x, result.y, result.z], full_range)]):
            error_msg = 'Some values out of range for experiment: {} (x, y, z)'.format(
                full_range)
            print(error_msg)
            raise ValueError(error_msg)

    return result, rmt


def main():
    result = collect_args()
    result, rmt = validate_args(result)

    print('Starting download')
    download_slices(result, rmt)
    print('Download complete')


if __name__ == '__main__':
    main()
