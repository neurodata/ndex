import unittest

import requests
import configparser

from boss_resources import *
# from intern.remote.boss import BossRemote

BOSS_URL = 'https://api.boss.neurodata.io/latest/'


class TestBossResources(unittest.TestCase):

    def setUp(self):
        # create a session for the BOSS using intern
        self.rmt = BossRemote('neurodata.cfg')

    def tearDown(self):
        pass

    def test_create_boss_res_params_just_names(self):

        coll_name = 'ben_dev'
        exp_name = 'dev_ingest_4'
        ch_name = 'def_files'

        boss_res_params = BossResParams(
            coll_name, exp_name, ch_name)
        self.assertEqual(boss_res_params.coll_name, coll_name)
        self.assertEqual(boss_res_params.exp_name, exp_name)
        self.assertEqual(boss_res_params.ch_name, ch_name)

    def test_get_existing_boss_res_from_just_names(self):
        parser = configparser.ConfigParser(interpolation=None)
        with open('neurodata.cfg') as f:
            parser.read_file(f)
        APITOKEN = parser['Default']['token']

        coll_name = 'ben_dev'
        exp_name = 'dev_ingest_4'
        ch_name = 'def_files'

        boss_res_params = BossResParams(
            coll_name, exp_name, ch_name)
        boss_res_params.setup_resources(self.rmt, get_only=True)

        headers = {'Authorization': 'Token {}'.format(APITOKEN)}
        url = '{}collection/{}/experiment/{}/channel/{}'.format(
            BOSS_URL, coll_name, exp_name, ch_name)
        r = requests.get(url, headers=headers)
        self.assertEqual(r.status_code, 200)

        # testing that what is returned by the web api is what we have in our boss resource
        response = r.json()
        self.assertEqual(response['experiment'],
                         boss_res_params.exp_resource.name)

        # testing that what is returned by intern for the channel name matches what is in our boss resource
        boss_ch_res = self.rmt.get_channel(ch_name, coll_name, exp_name)
        self.assertEqual(boss_ch_res.name, boss_res_params.ch_resource.name)

    def test_create_boss_res(self):
        coll_name = 'ben_dev'
        exp_name = 'dev_ingest_4'
        ch_name = 'def_files'

        voxel_size = [1, 1, 1]
        voxel_unit = 'nanometers'
        datatype = 'uint16'
        res = 0
        img_size = [2000, 1000, 100]

        boss_res_params = BossResParams(
            coll_name, exp_name, ch_name, voxel_size=voxel_size, voxel_unit=voxel_unit, datatype=datatype, res=res, img_size=img_size)
        boss_res_params.setup_resources(self.rmt, get_only=False)
        boss_ch_res = self.rmt.get_channel(ch_name, coll_name, exp_name)
        self.assertEqual(boss_ch_res.name, boss_res_params.ch_resource.name)


if __name__ == '__main__':
    unittest.main()
