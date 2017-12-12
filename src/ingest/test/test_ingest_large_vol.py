import os
import time
from argparse import Namespace
from datetime import datetime

import numpy as np
import pytest

from ....ingest_large_vol import (per_channel_ingest, post_cutout,
                                  read_channel_names)
from ..boss_resources import BossResParams
from ..ingest_job import IngestJob
from .create_images import del_test_images, gen_images


class TestIngestLargeVol:

    def setup(self):
        self.args = Namespace(datasource='local',
                              base_filename='img_<ch>_<p:4>',
                              base_path='local_img_test_data\\',
                              boss_config_file='neurodata.cfg',
                              collection='ben_dev',
                              experiment='dev_ingest_4',
                              x_extent=[0, 1000],
                              y_extent=[0, 1024],
                              z_extent=[0, 100],
                              z_range=[0, 2],
                              res=0,
                              voxel_size=[1.0, 1.0, 1.0],
                              voxel_unit='micrometers',
                              warn_missing_files=True,
                              z_step=1)

    def test_post_uint64_cutout(self):
        now = datetime.now()
        x_size = 64
        y_size = 64
        dtype = 'uint64'
        bit_width = int(''.join(filter(str.isdigit, dtype)))

        # generate a block of data
        data = np.zeros((self.args.z_range[1], y_size, x_size), dtype=dtype) + \
            np.random.randint(1, 2**bit_width - 1, dtype=dtype)

        # post (non-zero) data to boss
        st_x, sp_x, st_y, sp_y, st_z, sp_z = (
            0, x_size, 0, y_size, 0, self.args.z_range[1])

        self.args.z_range = [0, 1]
        self.args.datatype = dtype
        self.args.channel = 'def_files_annot_' + now.strftime("%Y%m%d-%H%M%S")
        self.args.source_channel = 'def_files'
        self.args.extension = 'tif'

        ingest_job = IngestJob(self.args)
        boss_res_params = BossResParams(ingest_job, get_only=False)

        ret_val = post_cutout(boss_res_params, ingest_job, [st_x, sp_x], [st_y, sp_y],
                              [st_z, sp_z], data, attempts=1)
        assert ret_val == 0

        # read data out of boss
        data_boss = boss_res_params.rmt.get_cutout(boss_res_params.ch_resource, 0,
                                                   [st_x, sp_x], [st_y, sp_y], [st_z, sp_z])
        # assert they are the same
        assert np.array_equal(data_boss, data)

        # cleanup
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        os.remove(ingest_job.get_log_fname())

    def test_post_uint16_cutout(self):
        x_size = 128
        y_size = 128
        dtype = 'uint16'
        bit_width = int(''.join(filter(str.isdigit, dtype)))

        # generate a block of data
        data = np.random.randint(
            1, 2**bit_width, size=(self.args.z_range[1], y_size, x_size), dtype=dtype)

        # post (non-zero) data to boss
        st_x, sp_x, st_y, sp_y, st_z, sp_z = (
            0, x_size, 0, y_size, 0, self.args.z_range[1])

        self.args.datatype = dtype
        self.args.channel = 'def_files'
        ingest_job = IngestJob(self.args)

        boss_res_params = BossResParams(ingest_job, get_only=True)

        ret_val = post_cutout(boss_res_params, ingest_job, [st_x, sp_x], [st_y, sp_y],
                              [st_z, sp_z], data, attempts=1)
        assert ret_val == 0

        # read data out of boss
        data_boss = boss_res_params.rmt.get_cutout(boss_res_params.ch_resource, 0,
                                                   [st_x, sp_x], [st_y, sp_y], [st_z, sp_z])
        # assert they are the same
        assert np.array_equal(data_boss, data)

        os.remove(ingest_job.get_log_fname())

    def test_post_uint16_cutout_offset_pixels(self):
        dtype = 'uint16'
        x_size = 128
        y_size = 128
        bit_width = int(''.join(filter(str.isdigit, dtype)))
        offset = 16

        # generate a block of data
        data = np.random.randint(
            1, 2**bit_width, size=(self.args.z_range[1], y_size, x_size), dtype=dtype)

        # post (non-zero) data to boss
        st_x, sp_x, st_y, sp_y, st_z, sp_z = (
            offset, offset + x_size,
            offset, offset + y_size,
            offset + self.args.z_range[0], offset + self.args.z_range[1])

        self.args.datatype = dtype
        self.args.channel = 'def_files'
        ingest_job = IngestJob(self.args)

        boss_res_params = BossResParams(ingest_job, get_only=True)

        ret_val = post_cutout(boss_res_params, ingest_job, [st_x, sp_x], [st_y, sp_y],
                              [st_z, sp_z], data, attempts=1)
        assert ret_val == 0

        # read data out of boss
        data_boss = boss_res_params.rmt.get_cutout(boss_res_params.ch_resource, 0,
                                                   [st_x, sp_x], [st_y, sp_y], [st_z, sp_z])
        # assert they are the same
        assert np.array_equal(data_boss, data)

        os.remove(ingest_job.get_log_fname())

    def test_ingest_uint8_annotations(self):
        dtype = 'uint8'
        now = datetime.now()

        self.args.base_filename = 'img_annotation_<p:4>'
        self.args.channel = 'def_files_annotation_' + \
            now.strftime("%Y%m%d-%H%M%S")
        self.args.channels_list_file = None
        self.args.source_channel = 'def_files'
        self.args.datatype = dtype
        self.args.extension = 'tif'
        self.args.create_resources = True

        ingest_job = IngestJob(self.args)

        gen_images(ingest_job,  intensity_range=30)

        channel = self.args.channel
        result = per_channel_ingest(self.args, channel)
        assert result == 0

        self.args.create_resources = False
        result = per_channel_ingest(self.args, channel)
        assert result == 0

        # cleanup
        boss_res_params = BossResParams(ingest_job, get_only=True)
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)

        del_test_images(ingest_job)
        os.remove(ingest_job.get_log_fname())

    def test_read_channel_names(self):
        channels_path = 'channels.example.txt'
        channels = read_channel_names(channels_path)

        valid_channels = ['Channel1', 'Channel0']
        assert valid_channels == channels

    def test_read_channel_names_no_channel_file(self):
        channels_path = 'channels.example_not_found.txt'

        with pytest.raises(FileNotFoundError):
            read_channel_names(channels_path)

    def test_per_channel_ingest(self):
        self.args.datatype = 'uint16'
        self.args.extension = 'tif'
        self.args.channels_list_file = 'channels.example.txt'

        channels = read_channel_names(self.args.channels_list_file)

        # assertions are inside ingest_test_per_channel

        # this is to create resources only:
        # self.ingest_test_per_channel(self.args, channels)

        self.args.create_resources = False
        self.ingest_test_per_channel(self.args, channels)

    def test_per_channel_ingest_wrong_datatype(self):
        # create 16 bit images and post to 8 bit resource
        now = datetime.now()
        self.args.channel = 'def_files_8bit_' + now.strftime("%Y%m%d-%H%M%S")
        self.args.extension = 'tif'

        # make 16 bit images
        args_uint16 = self.args
        args_uint16.datatype = 'uint16'
        ingest_job_uint16 = IngestJob(args_uint16)
        gen_images(ingest_job_uint16)

        # try to do an 8 bit ingest on a 16 bit resource with 16 bit files
        self.args.datatype = 'uint8'
        self.args.create_resources = True  # create the resource
        with pytest.raises(ValueError):
            per_channel_ingest(self.args, self.args.channel)

        # cleanup
        del_test_images(ingest_job_uint16)

        ingest_job = IngestJob(self.args)
        os.remove(ingest_job.get_log_fname())

    def test_per_channel_ingest_neg_xextent_no_offset(self):
        self.args.experiment = 'test_neg_extent_no_offset'
        self.args.channel = 'def_files'
        self.args.x_extent = [-1000, 0]
        self.args.offset_extents = False
        self.args.channels_list_file = 'channels.example.txt'

        channels = read_channel_names(self.args.channels_list_file)

        # assertions are inside ingest_test_per_channel

        # this is to create resources only:
        with pytest.raises(ValueError):
            self.ingest_test_per_channel(self.args, channels)

        self.args.create_resources = False
        with pytest.raises(ValueError):
            self.ingest_test_per_channel(self.args, channels)

    def test_per_channel_ingest_neg_x_exent_offset(self):
        now = datetime.now()

        self.args.experiment = 'test_neg_offset_' + \
            now.strftime("%Y%m%d-%H%M%S")
        self.args.channel = 'def_files'
        self.args.datatype = 'uint16'
        self.args.x_extent = [-1000, 0]
        self.args.offset_extents = True
        self.args.extension = 'png'
        self.args.channels_list_file = 'channels.example.txt'

        channels = read_channel_names(self.args.channels_list_file)

        # assertions are inside ingest_test_per_channel

        # this is to create resources only:
        self.args.create_resources = True
        self.ingest_test_per_channel(self.args, channels)

        self.args.create_resources = False
        self.ingest_test_per_channel(self.args, channels)

        # cleanup
        for ch in channels:
            ch_args = self.args
            ch_args.channel = ch
            ingest_job = IngestJob(ch_args)
            del_test_images(ingest_job)
            os.remove(ingest_job.get_log_fname())
            boss_res_params = BossResParams(ingest_job)
            boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        if len(channels) > 0:
            boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def test_per_channel_ingest_neg_z_exent_offset(self):
        now = datetime.now()

        self.args.experiment = 'test_neg_offset_' + \
            now.strftime("%Y%m%d-%H%M%S")
        self.args.channel = 'def_files'
        self.args.datatype = 'uint8'
        self.args.z_extent = [-100, 100]
        self.args.z_range = [-3, 2]
        self.args.offset_extents = True
        self.args.extension = 'png'

        ingest_job = IngestJob(self.args)
        gen_images(ingest_job)

        self.args.create_resources = True
        result = per_channel_ingest(self.args, self.args.channel)
        assert result == 0

        self.args.create_resources = False
        result = per_channel_ingest(self.args, self.args.channel)
        assert result == 0

        # cleanup
        del_test_images(ingest_job)
        os.remove(ingest_job.get_log_fname())
        boss_res_params = BossResParams(ingest_job)
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def ingest_test_per_channel(self, args, channels):
        for channel in channels:
            args.channel = channel
            ingest_job = IngestJob(args)
            gen_images(ingest_job)
            result = per_channel_ingest(args, channel)
            assert result == 0

    def test_ingest_render_stack(self):
        now = datetime.now()

        self.args.datasource = 'render'
        self.args.experiment = 'test_render_' + now.strftime("%Y%m%d-%H%M%S")
        self.args.channel = 'image_test_' + now.strftime("%Y%m%d-%H%M%S")
        self.args.datatype = 'uint8'
        self.args.render_owner = '6_ribbon_experiments'
        self.args.render_project = 'M321160_Ai139_smallvol'
        self.args.render_stack = 'Median_1_Gephyrin'
        self.args.render_baseURL = 'https://render-dev-eric.neurodata.io/render-ws/v1/'
        self.args.create_resources = True

        channel = self.args.channel
        result = per_channel_ingest(self.args, channel)
        assert result == 0

        self.args.create_resources = False
        result = per_channel_ingest(self.args, channel)
        assert result == 0

        boss_res_params = BossResParams(IngestJob(self.args), get_only=True)
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)
