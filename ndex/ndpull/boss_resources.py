import json
import time

import blosc
import numpy as np
import requests

BOSS_VERSION = "v1"


class BossMeta:
    def __init__(self, collection, experiment, channel, res=0, iso=False):
        self._collection = collection
        self._experiment = experiment
        self._channel = channel
        self._res = res
        self._iso = iso

    def channel(self):
        return self._channel

    def experiment(self):
        return self._experiment

    def collection(self):
        return self._collection

    def res(self):
        return self._res

    def iso(self):
        return self._iso


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

        self.boss_coll_metadata = self.get_coll_metadata()
        self.boss_exp_metadata = self.get_exp_metadata()
        self.boss_ch_metadata = self.get_channel_metdata()
        self.downsample_status = self.get_downsample_status()

    def __str__(self):
        string = 'Collection: {}, Experiment: {}, Channel: {}\n'.format(
            self.meta.collection(), self.meta.experiment(), self.meta.channel())

        indent_size = 2

        metadata = self.boss_exp_metadata
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Experiment metadata:\n{}\n\n'.format(metadata_str)

        metadata = self.get_coord_frame_metadata(exp_metadata=metadata)
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Coordinate frame metadata:\n{}\n\n'.format(metadata_str)

        metadata = self.boss_ch_metadata
        metadata_str = json.dumps(metadata, indent=indent_size)
        string += 'Channel metadata:\n{}\n\n'.format(metadata_str)

        return string

    def get(self, url, headers={}):
        if url[0] == '/':
            url = url[1:]
        r = self.session.get("{}{}".format(
            self.boss_url, url), headers=headers)
        return r

    def get_coll_metadata(self):
        # https://api.theboss.io/v1/collection/:collection/
        url = "{}/collection/{}".format(
            BOSS_VERSION, self.meta.collection()
        )
        resp = self.get(url, {'Accept': 'application/json'})
        return resp.json()

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

    def get_downsample_status(self):
        # https://api.boss.neurodata.io/v1/downsample/kristina15/image/CR1_2ndA
        url = "{}/downsample/{}/{}/{}".format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment(), self.meta.channel())
        resp = self.get(url, {'Accept': 'application/json'})
        return resp.json()

    def get_coord_frame_name(self, exp_data=None):
        if exp_data is None:
            exp_data = self.boss_exp_metadata
        return exp_data['coord_frame']

    def get_xyz_extents(self):
        coord_frame = self.get_coord_frame_metadata()
        x_rng = [coord_frame['x_start'], coord_frame['x_stop']]
        y_rng = [coord_frame['y_start'], coord_frame['y_stop']]
        z_rng = [coord_frame['z_start'], coord_frame['z_stop']]

        # extents are different in x/y for downsampled data
        x_rng, y_rng = [[round(bnd / 2**self.meta.res()) for bnd in rng]
                        for rng in [x_rng, y_rng]]

        if self.meta.iso():
            # need to convert to list to set new z_rng
            z_rng = [int(z / 2**self.meta.res()) for z in z_rng]

        return x_rng, y_rng, z_rng

    def cutout(self, x_rng, y_rng, z_rng, datatype, attempts=5):
        cutout_url_base = "{}/cutout/{}/{}/{}".format(
            BOSS_VERSION, self.meta.collection(), self.meta.experiment(), self.meta.channel())
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, self.meta.res(), x_rng[0], x_rng[1], y_rng[0], y_rng[1], z_rng[0], z_rng[1])
        if self.meta.iso():
            cutout_url += '?iso=True'

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
