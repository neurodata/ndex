'''
This script was used to copy the Hildebrand16 data 1k x 1k image tiles to the BOSS.
Run from the ndpush directory as module: python -m scripts.ingest_catmaid
'''

from argparse import Namespace
from functools import partial
from io import BytesIO
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import numpy as np
from PIL import Image

import sys
sys.path.append("..")

from ingest_large_vol import get_supercube_lims, post_cutout
from src.ingest.boss_resources import BossResParams
from src.ingest.ingest_job import IngestJob

# filenames on s3 (z/RES/y_x.png):
# 130201zf142/160515_SWiFT/60nmpx/10000/0/6_1.png
# URL:
# s3url = 'http://hildebrand16.s3-website-us-east-1.amazonaws.com/130201zf142/160515_SWiFT/60nmpx/{}/0/{}_{}.png'

owner = 'hildebrand'
project = '130201zf142'
channel = '160515_SWiFT_60nmpx'

x_width = 1024
y_width = 1024
x_extent_mult = 10
y_extent_mult = 9
z_extent = [2013, 18225]  # last exclusive
z_range = [12880, 18225]  # resume ingest

# test of one slice
# z_range = [12352, 12353]

# known missing
# z_range = [2803, 2804]


def get_data_boto3(z, y, x, datatype, s3):
    bucket_name = 'hildebrand16'
    key_str = '130201zf142/160515_SWiFT/60nmpx/{}/0/{}_{}.png'
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key_str.format(z, y, x))

        img = Image.open(BytesIO(obj['Body'].read()))
        return np.array(img, dtype=datatype)
    except ClientError as e:
        print(e)
        return np.zeros((y_width, x_width), dtype=datatype)


def main():

    args = Namespace(
        datasource='local',
        slack_usr='benfalk',
        s3_bucket_name=None,
        collection=owner,
        experiment=project,
        channel=channel,
        datatype='uint8',
        base_filename='',
        base_path='',
        extension='png',
        x_extent=[0, x_width * x_extent_mult],
        y_extent=[0, y_width * y_extent_mult],
        z_extent=z_extent,
        z_range=z_range,
        z_step=1,
        warn_missing_files=True,
        boss_config_file='neurodata.cfg',
        slack_token_file='slack_token',
        voxel_size=[56.4, 56.4, 60],
        voxel_unit='nanometers',
        res=0
    )

    ingest_job = IngestJob(args)
    boss_res_params = BossResParams(ingest_job)
    boss_res_params.get_resources(get_only=False)

    s3 = boto3.client('s3', region_name='us-east-1')

    # iterate over blocks of 16
    stride_size = 16
    z_buckets = get_supercube_lims(ingest_job.z_range, stride=stride_size)
    for _, slices in sorted(z_buckets.items()):
        print('iterating over z slices: {}:{} (inclusive)'.format(
            slices[0], slices[-1]))
        data = np.zeros((len(slices), ingest_job.y_extent[1], ingest_job.x_extent[1]),
                        dtype=ingest_job.datatype)

        # getting all the tiles within this section of z slices
        pool_args = []
        for z in slices:
            for y in range(y_extent_mult):
                for x in range(x_extent_mult):
                    # create args for extracting tiles
                    pool_args.append((z, y, x, ingest_job.datatype, s3))

        threads = 60
        with ThreadPool(threads) as pool:
            data_array = pool.starmap(get_data_boto3, pool_args)

        for idx, a in enumerate(pool_args):
            zz, yy, xx, _, _ = a
            data[zz-slices[0],
                 yy * y_width:yy * y_width + y_width,
                 xx * x_width:xx * x_width + x_width] = data_array[idx]

        # create an image here to look at it before we start posting data to boss
        # full_img = Image.fromarray(data[0, :, :])
        # full_img.save('test_img.png')

        x_rng = ingest_job.x_extent
        y_rng = ingest_job.y_extent
        z_rng = [slices[0], slices[-1] + 1]

        block_size = 1024  # evenly divisible into x/y widths
        pool_args = []
        for yy in range(y_rng[0], y_rng[1], block_size):
            yy_rng = [yy, yy + block_size]
            for xx in range(x_rng[0], x_rng[1], block_size):
                xx_rng = [xx, xx + block_size]
                sub_data = data[:,
                                yy_rng[0] - y_rng[0]:yy_rng[1] - y_rng[0],
                                xx_rng[0] - x_rng[0]:xx_rng[1] - x_rng[0]]
                sub_data = np.asarray(sub_data, order='C')
                pool_args.append((boss_res_params, ingest_job,
                                  xx_rng, yy_rng, z_rng, sub_data))

        # print(pool_args)
        threads = 8
        with ThreadPool(threads) as pool:
            pool.starmap(post_cutout, pool_args)
        pool_args = []


if __name__ == '__main__':
    main()
