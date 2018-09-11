'''
Command line program to download a region of data from the BOSS as individual TIFF slices
'''

import argparse
import configparser
import json
import math
import os
import sys
import time
from collections import defaultdict
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path

import blosc
import numpy as np
import requests
import tifffile as tiff
from tqdm import tqdm

from ndex.ndpull.boss_resources import *

# download blocks of size 2k by 2k by 16 (xyz), should be multiples of 512 x 512 x 16
CHUNK_SIZE = (2048, 2048, 16)


def get_cube_lims(rng, stride=16):
    # stride = height of super cuboid

    first = rng[0]    # inclusive
    last = rng[1]     # exclusive

    buckets = defaultdict(list)
    for z in range(first, last):
        buckets[(z // stride)].append(z)

    return buckets


def collect_input_args(collection, experiment, channel, config_file=None, token=None, url='https://api.boss.neurodata.io', x=None, y=None, z=None, res=0, outdir='./', full_extent=False, print_metadata=False, iso=False, force_datatype=False):
    result = argparse.Namespace(
        collection=collection,
        experiment=experiment,
        channel=channel,
        config_file=config_file,
        token=token,
        url=url,
        x=x, y=y, z=z, res=res, outdir=outdir,
        full_extent=full_extent,
        print_metadata=print_metadata,
        iso=iso,
        force_datatype=force_datatype,
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
    parser.add_argument('--threads', default=4, type=int,
                        help='Number of threads for downloading data.')
    parser.add_argument('--iso', action='store_true',
                        help='Returns iso data (for downsampling in z)')

    parser.add_argument('--stack_filename', type=str,
                        help='If specified, tiffs are merged into a single tif stack file, at the outdir specified')

    parser.add_argument('--force_datatype', type=str,
                        help='downloaded data will be cast into this datatype (uint8/uint16/uint32)')

    return parser.parse_args()


def get_boss_config(boss_config_file=None):
    config = configparser.ConfigParser()

    if boss_config_file:
        config.read(boss_config_file)
        token = config['Default']['token']
        protocol = config['Default']['protocol']
        host = config['Default']['host']
    else:
        token = os.environ['BOSS_TOKEN']
        protocol = 'https'
        host = 'api.boss.neurodata.io'

    boss_url = ''.join((protocol, '://', host))
    return token, boss_url


def download_slices(result, rmt, threads=4):

    # get the datatype
    ch_meta = rmt.boss_ch_metadata
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
                rmt.cutout, y_rng=y_rng, z_rng=z_rng, datatype=datatype)
            with ThreadPool(threads) as pool:
                data_list = pool.map(cutout_partial, x_rngs)

            for data, x_rng in zip(data_list, x_rngs):
                # insert into numpy array
                data_slices[:,
                            y_rng[0] - result.y[0]:y_rng[1] - result.y[0],
                            x_rng[0] - result.x[0]:x_rng[1] - result.x[0]] = data

        save_to_tiffs(data_slices, rmt.meta, result,
                      z_rng, result.force_datatype)


def gen_tif_fname(meta, result, zslice, digits):
    file_format = '{}_{}_{}_x{x[0]}-{x[1]}_y{y[0]}-{y[1]}_z{z:0{dig}d}.tif'
    fname = file_format.format(
        meta.collection(), meta.experiment(), meta.channel(),
        x=result.x, y=result.y, z=zslice, dig=digits)
    return fname


def save_to_tiffs(data_slices, meta, result, z_rng, force_datatype=None):
    # save the numpy array as a tiff file

    # path for saving slices
    cutout_path = Path(result.outdir)
    cutout_path.mkdir(parents=True, exist_ok=True)

    digits = int(math.log10(result.z[1])) + 1

    for zslice in range(z_rng[0], z_rng[1]):
        fname = gen_tif_fname(meta, result, zslice, digits)

        data = data_slices[zslice - z_rng[0], :, :]

        if force_datatype:
            data = data.astype(force_datatype)

        tiff.imsave(str(cutout_path / fname), data,
                    metadata={'DocumentName': fname}, compress=6)


def save_to_stack(meta, result):
    # save the extracted slices to a single tiff stack

    cutout_path = Path(result.outdir)

    stack_fname = Path(cutout_path, result.stack_filename)

    digits = int(math.log10(result.z[1])) + 1

    try:
        stack_fname.unlink()
    except OSError:
        pass
    for zslice in range(result.z[0], result.z[-1]):
        img_fname = gen_tif_fname(meta, result, zslice, digits)

        I = tiff.imread(str(cutout_path / img_fname))
        tiff.imsave(str(stack_fname), I, append=True)
        Path(cutout_path / img_fname).unlink()

# to add:
# tracking of amount of data requests (with default limit, so it can stop if it passes a threshold)


def validate_args(result):
    # get tokens from config file if set as an option
    result.token, result.url = get_boss_config(result.config_file)
    if result.token is None:
        error_msg = 'Need token or config file'
        print(error_msg)
        raise ValueError(error_msg)

    meta = BossMeta(result.collection, result.experiment,
                    result.channel, result.res, result.iso)
    rmt = BossRemote(result.url, result.token, meta)

    if result.print_metadata:
        print(rmt)
        sys.exit()

    if meta.res() > 0:
        if meta.res() >= rmt.boss_exp_metadata['num_hierarchy_levels']:
            raise ValueError('Res argument too high for experiment')

        if rmt.downsample_status['status'] != 'DOWNSAMPLED':
            raise ValueError('Experiment not downsampled')

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
    args = collect_args()
    result, rmt = validate_args(args)

    print('Starting download')
    download_slices(result, rmt, threads=args.threads)
    print('Download complete')

    if result.stack_filename:
        save_to_stack(rmt.meta, result)


if __name__ == '__main__':
    main()
