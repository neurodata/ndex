import math
import os
import re
import time
import unittest
from argparse import Namespace
from datetime import datetime

import boto3
import numpy as np
import png
import tifffile as tiff

from ingest_large_vol import *


class TestIngestLargeVol(unittest.TestCase):

    def setUp(self):
        self.startTime = time.time()

        self.rmt = BossRemote('neurodata.cfg')

        coll_name = 'ben_dev'
        exp_name = 'dev_ingest_4'
        ch_name = 'def_files'

        self.x_size = 1000
        self.y_size = 1024
        self.dtype = 'uint16'

        self.boss_res_params = BossResParams(
            coll_name, exp_name, ch_name, datatype=self.dtype)

        self.z = 0
        self.z_rng = [0, 16]
        self.fileprefix = 'img_<p:4>'
        self.data_directory = 'local_img_test_data\\'
        self.z_step = 1

    def tearDown(self):
        t = time.time() - self.startTime
        print('{:03.1f}s: {}'.format(t, self.id()))

    def test_send_msg(self):
        msg = 'test_message_' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        send_msg(self.boss_res_params, msg)

        log_fname = gen_log_fname(self.boss_res_params)
        with open(log_fname) as f:
            log_data = f.readlines()

        self.assertIn(msg, log_data[-1])

    def test_get_img_fname(self):
        file_format = 'tif'
        img_fname = get_img_fname(
            self.fileprefix, self.data_directory, file_format, self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)

        img_fname_test = '{}img_{:04d}.tif'.format(self.data_directory, self.z)
        self.assertEqual(img_fname, img_fname_test)

    def test_get_img_info_uint8_tif(self):
        file_format = 'tif'
        dtype = 'uint8'

        img_fname = get_img_fname(
            self.fileprefix, self.data_directory, file_format, self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        create_img_file(self.x_size, self.y_size,
                        dtype, file_format, img_fname)

        im_width, im_height, im_datatype = get_img_info(
            self.boss_res_params, img_fname)
        self.assertEqual(im_width, self.x_size)
        self.assertEqual(im_height, self.y_size)
        self.assertEqual(im_datatype, dtype)

    def test_get_img_info_uint16_tif(self):
        file_format = 'tif'

        img_fname = get_img_fname(
            self.fileprefix, self.data_directory, file_format, self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        create_img_file(self.x_size, self.y_size,
                        self.dtype, file_format, img_fname)

        im_width, im_height, im_datatype = get_img_info(
            self.boss_res_params, img_fname)
        self.assertEqual(im_width, self.x_size)
        self.assertEqual(im_height, self.y_size)
        self.assertEqual(im_datatype, self.dtype)

    def test_get_img_info_uint16_tif_s3(self):
        file_format = 'tif'
        bucket_name = 'benfalk-dev'

        # create an image
        img_fname = get_img_fname(self.fileprefix, self.data_directory, file_format,
                                  self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        create_img_file(self.x_size, self.y_size,
                        self.dtype, file_format, img_fname)

        # put the image on a bucket
        s3 = boto3.resource('s3')
        data = open(img_fname, 'rb')
        s3_fname = 'tests/' + img_fname
        s3.Bucket(bucket_name).put_object(Key=s3_fname, Body=data)

        # get info on that image
        im_width, im_height, im_datatype = get_img_info(
            self.boss_res_params, s3_fname, s3_res=s3, s3_bucket_name=bucket_name)

        # assert the info is correct
        self.assertEqual(im_width, self.x_size)
        self.assertEqual(im_height, self.y_size)
        self.assertEqual(im_datatype, self.dtype)

        # closing the boto3 session
        s3.meta.client._endpoint.http_session.close()

    def test_get_img_info_uint16_png(self):
        file_format = 'png'

        img_fname = get_img_fname(
            self.fileprefix, self.data_directory, file_format, self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        create_img_file(self.x_size, self.y_size,
                        self.dtype, file_format, img_fname)

        im_width, im_height, im_datatype = get_img_info(
            self.boss_res_params, img_fname)
        self.assertEqual(im_width, self.x_size)
        self.assertEqual(im_height, self.y_size)
        self.assertEqual(im_datatype, self.dtype)

    def test_get_img_info_uint64_tif(self):
        file_format = 'tif'
        dtype = 'uint64'

        img_fname = get_img_fname(
            self.fileprefix, self.data_directory, file_format, self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        create_img_file(self.x_size, self.y_size,
                        dtype, file_format, img_fname)

        im_width, im_height, im_datatype = get_img_info(
            self.boss_res_params, img_fname)
        self.assertEqual(im_width, self.x_size)
        self.assertEqual(im_height, self.y_size)
        self.assertEqual(im_datatype, dtype)

    def test_read_uint16_img_stack(self):

        # generate some images
        file_format = 'tif'
        gen_images(self.z_rng[1], self.x_size, self.y_size,
                   self.dtype, file_format, self.fileprefix, self.data_directory)

        # load images into memory using ingest_large_vol function
        s3_res = None
        s3_bucket_name = None

        z_slices = range(self.z_rng[0], self.z_rng[1])
        z_step = 1
        self.boss_res_params.setup_resources(self.rmt)
        im_array = read_img_stack(self.boss_res_params, z_slices, self.fileprefix, self.data_directory,
                                  file_format, s3_res, s3_bucket_name, self.z_rng, self.z_step)

        # check to make sure each image is equal to each z index in the array
        for z in z_slices:
            img_fname = self.data_directory + 'img_{:04d}.tif'.format(z)
            im = Image.open(img_fname)
            self.assertTrue(np.array_equal(im_array[z, :, :], im))

    def test_post_uint64_cutout(self):
        x_size = 128
        y_size = 128
        dtype = 'uint64'
        bit_width = int(''.join(filter(str.isdigit, dtype)))

        # generate a block of data
        # data = np.random.randint(
        #     1, 2**bit_width, size=(self.z_rng[1], y_size, x_size), dtype=dtype)
        data = np.zeros((self.z_rng[1], y_size, x_size), dtype=dtype) + \
            np.random.randint(1, 2**bit_width, dtype=dtype)

        # post (non-zero) data to boss
        st_x, sp_x, st_y, sp_y, st_z, sp_z = (
            0, x_size, 0, y_size, 0, self.z_rng[1])

        coll_name = 'ben_dev'
        exp_name = 'dev_ingest_4'
        ch_name = 'def_files_annotation'
        boss_res_params = BossResParams(coll_name, exp_name, ch_name, voxel_size=[
                                        1, 1, 1], voxel_unit='micrometers', datatype=dtype, res=0, img_size=[1000, 1024, 100], source='def_files')
        boss_res_params.setup_resources(self.rmt, get_only=False)
        ret_val = post_cutout(boss_res_params, st_x, sp_x, st_y, sp_y,
                              st_z, sp_z, data, attempts=2, slack=None, slack_usr=None)

        self.assertEqual(ret_val, 0)

        # read data out of boss
        data_boss = self.rmt.get_cutout(boss_res_params.ch_resource, 0,
                                        [0, x_size], [0, y_size], self.z_rng)

        # assert they are the same
        self.assertTrue(np.array_equal(data_boss, data))

    def test_post_uint16_cutout(self):
        x_size = 128
        y_size = 128
        dtype = 'uint16'
        bit_width = int(''.join(filter(str.isdigit, dtype)))

        # generate a block of data
        data = np.random.randint(
            1, 2**bit_width, size=(self.z_rng[1], y_size, x_size), dtype=dtype)

        # post (non-zero) data to boss
        st_x, sp_x, st_y, sp_y, st_z, sp_z = (
            0, x_size, 0, y_size, 0, self.z_rng[1])

        self.boss_res_params.setup_resources(self.rmt)
        ret_val = post_cutout(self.boss_res_params, st_x, sp_x, st_y, sp_y,
                              st_z, sp_z, data, attempts=2, slack=None, slack_usr=None)

        self.assertEqual(ret_val, 0)

        # read data out of boss
        data_boss = self.rmt.get_cutout(self.boss_res_params.ch_resource, 0,
                                        [0, x_size], [0, y_size], self.z_rng)

        # assert they are the same
        self.assertTrue(np.array_equal(data_boss, data))

    def test_read_channel_names(self):
        channels_path = 'channels.example.txt'
        channels = read_channel_names(channels_path)

        valid_channels = ['Channel1', 'Channel0']
        self.assertEqual(valid_channels, channels)

    def test_read_channel_names_no_channel_file(self):
        channels_path = 'channels.example_not_found.txt'

        self.assertRaises(FileNotFoundError, read_channel_names, channels_path)

    def test_get_img_fname_channel(self):
        fileprefix = 'img_<ch>_<p:4>'
        data_directory = 'local_img_<ch>_test_data\\'
        extension = 'tif'
        img_fname = get_img_fname(fileprefix, data_directory, extension,
                                  self.z, self.z_rng, self.z_step, self.boss_res_params.ch_name)
        print(img_fname)
        self.assertEqual(img_fname, 'local_img_{0}_test_data\\img_{0}_{1:04d}.tif'.format(
            self.boss_res_params.ch_name, self.z))

    def test_per_channel_ingest(self):
        args = Namespace(aws_profile='default',
                         base_filename='img_<ch>_<p:4>',
                         base_path='local_img_test_data\\',
                         boss_config_file='neurodata.cfg',
                         channel=None,
                         channels_list_file='channels.example.txt',
                         collection='ben_dev',
                         create_resources=True,
                         datasource='local',
                         datatype='uint16',
                         experiment='dev_ingest_4',
                         extension='tif',
                         img_size=[1000, 1024, 100],
                         res=0,
                         s3_bucket_name=None,
                         slack_token_file='slack_token',
                         slack_usr=None,
                         voxel_size=[1.0, 1.0, 1.0],
                         voxel_unit='micrometers',
                         warn_missing_files=True,
                         z_range=[0, 16],
                         z_step=1,
                         source_channel=None)

        channels = read_channel_names(args.channels_list_file)
        ingest_per_channel(args, channel)

        args.create_resources = False
        ingest_per_channel(args, channel)

    def ingest_per_channel(self, args, channel):
        for channel in channels:
            gen_images(self.z_rng[1], self.x_size, self.y_size,
                       args.datatype, args.extension, args.base_filename, args.base_path, channel=channel)
            result = per_channel_ingest(args, channel)
            self.assertEqual(result, 0)

    def test_ingest_uint8_annotations(self):
        args = Namespace(base_filename='img_annotation_<p:4>',
                         base_path='local_img_test_data\\',
                         boss_config_file='neurodata.cfg',
                         channel='def_files_annotation',
                         channels_list_file=None,
                         collection='ben_dev',
                         create_resources=True,
                         datasource='local',
                         datatype='uint8',
                         experiment='dev_ingest_4',
                         extension='tif',
                         img_size=[1000, 1024, 100],
                         res=0,
                         s3_bucket_name=None,
                         slack_token_file='slack_token',
                         slack_usr=None,
                         voxel_size=[1.0, 1.0, 1.0],
                         voxel_unit='micrometers',
                         warn_missing_files=True,
                         z_range=[0, 16],
                         z_step=1,
                         source_channel='dev_ingest_4')

        file_format = 'tif'
        file_prefix = 'img_annotation_<p:4>'
        dtype = 'uint8'
        gen_images(self.z_rng[1], self.x_size, self.y_size,
                   dtype, file_format, file_prefix, self.data_directory, intensity_range=30)

        channel = args.channel
        result = per_channel_ingest(args, channel)
        self.assertEqual(0, result)

        args.create_resources = False
        result = per_channel_ingest(args, channel)
        self.assertEqual(0, result)


def create_img_file(x_size, y_size, dtype, file_format, img_fname, intensity_range=None):
    if intensity_range is None:
        bit_width = int(''.join(filter(str.isdigit, dtype)))
    else:
        bit_width = round(math.log(intensity_range, 2))
    ar = np.random.randint(
        1, 2**bit_width, size=(y_size, x_size), dtype=dtype)

    directory = os.path.dirname(img_fname)
    if not os.path.isdir(directory):
        os.makedirs(directory)

    if file_format == 'tif':
        tiff.imsave(img_fname, ar)
    elif file_format == 'png':
        with open(img_fname, 'wb') as f:
            writer = png.Writer(width=x_size, height=y_size,
                                bitdepth=bit_width, greyscale=True)
            writer.write(f, ar.tolist())


def gen_images(n_images, x_size, y_size, dtype, file_format, fileprefix, directory, z_step=1, channel='Ch1', intensity_range=None):
    for z in range(0, n_images * z_step, z_step):
        img_fname = get_img_fname(
            fileprefix, directory, file_format, z, [0, n_images], z_step, channel)
        create_img_file(x_size, y_size, dtype, file_format,
                        img_fname, intensity_range)


if __name__ == '__main__':
    unittest.main()
