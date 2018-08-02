'''
Command line program to download supercubes (512x512x16, x/y/z) from BOSS volume as individual TIFFs
'''

import argparse
import sys

import blosc
import numpy as np
import requests
from PIL import Image

BOSS_VERSION = "v1"


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

    def get(self, url, input_headers={}):
        if url[0] == '/':
            url = url[1:]
        if len(input_headers) > 0:
            headers = input_headers
        else:
            headers = {}
        headers['Authorization'] = 'Token {}'.format(self.token)
        r = requests.get("{}{}".format(self.boss_url, url), headers=headers)
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

    def get_coord_frame_metadata(self):
        coord_frame_name = self.get_coord_frame_name()
        coord_frame_url = "{}/coord/{}".format(BOSS_VERSION, coord_frame_name)
        resp = self.get(coord_frame_url, {'Accept': 'application/json'})
        return resp.json()

    def get_coord_frame_name(self):
        exp_data = self.get_exp_metadata()
        return exp_data['coord_frame']

    def get_xyz_extents(self):
        coord_frame = self.get_coord_frame_metadata()
        x_rng = [coord_frame['x_start'], coord_frame['x_stop']]
        y_rng = [coord_frame['y_start'], coord_frame['y_stop']]
        z_rng = [coord_frame['z_start'], coord_frame['z_stop']]
        return x_rng, y_rng, z_rng

    def cutout(self, x_rng, y_rng, z_rng, res=0, datatype=np.uint8):
        cutout_url_base = "{}/cutout/{}/{}/{}".format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment(), self.meta.channel())
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, res, x_rng[0], x_rng[1], y_rng[0], y_rng[1], z_rng[0], z_rng[1])
        print(cutout_url)
        resp = self.get(cutout_url, {'Accept': 'application/blosc'})
        assert(resp.status_code == 200)
        raw_data = blosc.decompress(resp.content)
        data = np.fromstring(raw_data, dtype=datatype)
        # We don't have any datasets that require 64-bits for annotation labels,
        # so just convert to 32-bit here.
        if datatype == "uint64":
            data = data.astype(np.uint32)
        return np.reshape(data,
                          (z_rng[1] - z_rng[0],
                           y_rng[1] - y_rng[0],
                           x_rng[1] - x_rng[0]),
                          order='C')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('token', type=str, help='User token for the boss.')
    parser.add_argument('collection', type=str, help='Collection')
    parser.add_argument('experiment', type=str, help='Experiment')
    parser.add_argument('channel', type=str, help='Channel')

    parser.add_argument(
        '--url', default='https://api.boss.neurodata.io', help='URL to boss endpoint.')
    parser.add_argument('--x', nargs=2, type=int, help='X range for stack')
    parser.add_argument('--y', nargs=2, type=int, help='Y range for stack')
    parser.add_argument('--z', nargs=2, type=int, help='Z range for stack')
    parser.add_argument('--res', default=0, type=int, help='Stack resolution')

    parser.add_argument('--datatype', type=str,
                        choices=["uint8", "uint32", "uint64"], default="uint8")
    parser.add_argument('--outdir', type=str, default="./",
                        help='Path to output directory.')

    parser.add_argument('--full_extent', action='store_true',
                        help='Use the full extent of the data on the BOSS')

    result = parser.parse_args()

    meta = BossMeta(result.collection, result.experiment, result.channel)

    rmt = BossRemote(result.url, result.token, meta)

    if result.full_extent:
        if any([a is not None for a in [result.x, result.y, result.z]]):
            print('full extent not compatible with specified x, y, or z ranges')
            sys.exit()
        result.x, result.y, result.z = rmt.get_xyz_extents()
    else:
        if any([a is None for a in [result.x, result.y, result.z]]):
            print('Need extents for x, y, and z for download')
            sys.exit()

    ch_meta = rmt.get_channel_metdata()
    result.datatype = ch_meta['datatype']

    # set the chunk size to 512 by 512 by 16
    chunk_size = (512, 512, 16)
    for z in range(result.z[0], result.z[1], chunk_size[2]):
        for y in range(result.y[0], result.y[1], chunk_size[1]):
            for x in range(result.x[0], result.x[1], chunk_size[0]):
                x_rng = [x, result.x[1] if (
                    x + chunk_size[0]) > result.x[1] else (x + chunk_size[0])]
                y_rng = [y, result.y[1] if (
                    y + chunk_size[1]) > result.y[1] else (y + chunk_size[1])]
                z_rng = [z, result.z[1] if (
                    z + chunk_size[2]) > result.z[1] else (z + chunk_size[2])]

                cutout_str = "{}-{}_{}-{}_{}-{}.tif".format(
                    x_rng[0], x_rng[1], y_rng[0], y_rng[1], z_rng[0], z_rng[1])
                data = rmt.cutout(x_rng, y_rng, z_rng,
                                  result.res, result.datatype)

                if not np.any(data):
                    # skip cubes containing all 0s
                    continue

                imlist = []

                cutout_path = result.outdir
                if cutout_path[-1] != '/':
                    cutout_path = cutout_path + '/'
                cutout_path = "{}{}".format(cutout_path, cutout_str)
                for slab in data:
                    imlist.append(Image.fromarray(slab))

                if len(imlist) > 1:
                    imlist[0].save(cutout_path, compression="tiff_deflate",
                                   save_all=True, append_images=imlist[1:])
                else:
                    imlist[0].save(cutout_path)


if __name__ == '__main__':
    main()
