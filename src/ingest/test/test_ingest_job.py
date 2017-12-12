import os
from argparse import Namespace
from datetime import datetime

import boto3
import numpy as np
import pytest
from PIL import Image

from ..ingest_job import IngestJob
from .create_images import create_img_file, del_test_images, gen_images


class TestIngestJob:
    def setup(self):
        # setting up the most commmon args
        self.args = Namespace(
            datasource='local',
            slack_usr='benfalk',
            slack_token_file='slack_token',
            s3_bucket_name=None,
            collection='ben_dev',
            experiment='dev_ingest_4',
            channel='def_files',
            datatype='uint16',
            base_filename='img_<p:4>',
            base_path='local_img_test_data\\',
            extension='tif',
            x_extent=[0, 1000],
            y_extent=[0, 1024],
            z_extent=[0, 100],
            z_range=[0, 1],
            z_step=1,
            warn_missing_files=True)

    def test_create_local_IngestJob(self):
        ingest_job = IngestJob(self.args)

        assert ingest_job.x_extent == self.args.x_extent
        assert ingest_job.base_fname == self.args.base_filename

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_no_extents(self):
        self.args.x_extent = None
        self.args.y_extent = None
        self.args.z_extent = None

        ingest_job = IngestJob(self.args)

        assert ingest_job.x_extent is None
        assert ingest_job.y_extent is None
        assert ingest_job.z_extent is None
        assert ingest_job.img_size is None

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_neg_extents(self):
        self.args.x_extent = [-1000, 0]

        with pytest.raises(ValueError, message='Extents must be positive for the BOSS'):
            IngestJob(self.args)

    def test_create_local_IngestJob_neg_extents_offset(self):
        self.args.x_extent = [-1000, -100]
        self.args.offset_extents = True

        ingest_job = IngestJob(self.args)

        assert ingest_job.offsets == [1000, 0, 0]
        assert ingest_job.x_extent == [0, 900]

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_pos_extents_offset(self):
        self.args.x_extent = [1000, 2000]
        self.args.offset_extents = True

        ingest_job = IngestJob(self.args)

        assert ingest_job.offsets == [0, 0, 0]
        assert ingest_job.x_extent == [1000, 2000]

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_neg_z_extents_offset_range(self):
        self.args.z_extent = [-1000, 2000]
        self.args.z_range = [-10, 10]
        self.args.offset_extents = True

        ingest_job = IngestJob(self.args)

        assert ingest_job.x_extent == [0, 1000]
        assert ingest_job.offsets == [0, 0, 1000]
        assert ingest_job.z_extent == [0, 3000]
        assert ingest_job.z_range == [-10, 10]

        img_fname = ingest_job.get_img_fname(-5)
        # assert that first tif image has a file name with the negative original extent of the data
        img_fname_test = '{}img_{:04d}.{}'.format(
            self.args.base_path, -5, self.args.extension)
        assert img_fname == img_fname_test

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_annotation(self):
        self.args.source_channel = 'def_files'
        self.args.datatype = 'uint64'

        ingest_job = IngestJob(self.args)

        assert ingest_job.source_channel == self.args.source_channel
        assert ingest_job.boss_datatype == 'uint64'
        assert ingest_job.datatype == 'uint64'

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_annotation_uint32(self):
        self.args.source_channel = 'def_files'
        self.args.datatype = 'uint32'

        ingest_job = IngestJob(self.args)

        assert ingest_job.source_channel == self.args.source_channel
        assert ingest_job.boss_datatype == 'uint64'
        assert ingest_job.datatype == 'uint32'

        os.remove(ingest_job.get_log_fname())

    def test_create_local_IngestJob_annotation_uint32_no_source_channel(self):
        self.args.datatype = 'uint32'

        with pytest.raises(ValueError):
            ingest_job = IngestJob(self.args)

    def test_create_s3_IngestJob(self):
        pass

    def test_create_render_IngestJob(self):
        self.set_render_args()
        ingest_job = IngestJob(self.args)

        assert ingest_job.render_obj.scale == 1
        assert ingest_job.render_window is None

        assert ingest_job.x_extent == [0, 5608]
        assert ingest_job.y_extent == [0, 2049]
        assert ingest_job.z_extent == [0, 536]

        assert ingest_job.render_obj.tile_width == 2048
        assert ingest_job.render_obj.tile_height == 2047

        assert ingest_job.z_range == [0, 1]  # from our params in setup
        os.remove(ingest_job.get_log_fname())

    def test_create_render_scale_quarter_IngestJob(self):
        self.set_render_args()
        self.args.render_scale = .25
        ingest_job = IngestJob(self.args)

        assert ingest_job.render_obj.scale == self.args.render_scale

        assert ingest_job.x_extent == [0, round(5608 * self.args.render_scale)]
        assert ingest_job.y_extent == [0, round(2049 * self.args.render_scale)]
        assert ingest_job.z_extent == [0, 536]

        assert ingest_job.render_obj.tile_width == 2048
        assert ingest_job.render_obj.tile_height == 2047

        assert ingest_job.z_range == [0, 1]  # from our params in setup
        os.remove(ingest_job.get_log_fname())

    def test_create_render_window_IngestJob(self):
        self.set_render_args()
        self.args.render_window = [50, 1000]

        ingest_job = IngestJob(self.args)

        assert ingest_job.render_window == self.args.render_window
        os.remove(ingest_job.get_log_fname())

    def set_render_args(self):
        self.args.datasource = 'render'
        self.args.experiment = 'test_render'
        self.args.channel = 'image_test'
        self.args.datatype = 'uint8'
        self.args.render_owner = '6_ribbon_experiments'
        self.args.render_project = 'M321160_Ai139_smallvol'
        self.args.render_stack = 'Median_1_Gephyrin'
        self.args.render_baseURL = 'https://render-dev-eric.neurodata.io/render-ws/v1/'

    def test_send_msg(self):
        ingest_job = IngestJob(self.args)

        msg = 'test_message_' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ingest_job.send_msg(msg)

        log_fname = ingest_job.get_log_fname()
        with open(log_fname) as f:
            log_data = f.readlines()

        assert msg in log_data[-1]

        os.remove(log_fname)

    def test_send_msg_slack(self):
        self.args.slack_usr = 'benfalk'
        ingest_job = IngestJob(self.args)

        assert ingest_job.slack_obj is not None

        msg = 'test_message_' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ingest_job.send_msg(msg, send_slack=True)

        log_fname = ingest_job.get_log_fname()
        os.remove(log_fname)

    def test_get_img_fname(self):
        self.args.extension = 'tif'
        ingest_job = IngestJob(self.args)

        img_fname = ingest_job.get_img_fname(0)

        img_fname_test = '{}img_{:04d}.{}'.format(
            self.args.base_path, 0, self.args.extension)
        assert img_fname == img_fname_test
        os.remove(ingest_job.get_log_fname())

    def test_get_img_fname_render(self):
        self.set_render_args()
        ingest_job = IngestJob(self.args)

        img_fname = ingest_job.get_img_fname(0)

        assert img_fname is None
        os.remove(ingest_job.get_log_fname())

    def test_load_img_local(self):
        ingest_job = IngestJob(self.args)

        z_slice = 0

        img_fname = ingest_job.get_img_fname(z_slice)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, img_fname)

        im = ingest_job.load_img(z_slice)

        img_local_test = np.array(Image.open(img_fname))

        assert np.array_equal(im, img_local_test)
        os.remove(ingest_job.get_log_fname())

    def test_load_img_s3(self):
        # currently contained in the load_img_info_s3 test
        pass

    def test_get_img_info_render(self):
        self.set_render_args()
        ingest_job = IngestJob(self.args)

        z_slice = 0
        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)

        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def test_get_img_info_render_neg_extents(self):
        self.set_render_args()
        self.args.offset_extents = True
        self.args.render_stack = 'Stitched_DAPI_1_Lowres_RoughAligned'

        ingest_job = IngestJob(self.args)

        assert ingest_job.render_obj.x_rng_unscaled == [-3534, 12469]
        assert ingest_job.render_obj.y_rng_unscaled == [-7196, 7734]

        z_slice = ingest_job.z_range[0]
        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)

        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def test_get_img_info_uint8_tif(self):
        dtype = 'uint8'
        self.args.datatype = dtype
        ingest_job = IngestJob(self.args)

        z_slice = 0

        img_fname = ingest_job.get_img_fname(z_slice)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, img_fname)

        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)
        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def test_get_img_info_uint16_tif(self):
        dtype = 'uint16'
        self.args.datatype = dtype
        ingest_job = IngestJob(self.args)

        z_slice = 0

        img_fname = ingest_job.get_img_fname(z_slice)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, img_fname)

        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)
        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def set_s3_args(self):
        base_path = 'tests/'
        base_fn = 'img_s3_<p:4>'
        bucket_name = 'benfalk-dev'

        self.args.datasource = 's3'
        self.args.experiment = 'test_s3'
        self.args.channel = 'image_test'
        self.args.s3_bucket_name = bucket_name
        self.args.base_path = base_path
        self.args.base_filename = base_fn

    def test_get_img_info_uint16_tif_s3(self):
        self.set_s3_args()
        ingest_job = IngestJob(self.args)

        z_slice = 0

        # create an image
        local_base_path = 'local_img_test_data\\'

        img_fname = ingest_job.get_img_fname(z_slice)
        img_fname_only = os.path.basename(img_fname)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, local_base_path + img_fname_only)

        # put the image on a bucket
        s3 = boto3.resource('s3')
        data = open(local_base_path + img_fname_only, 'rb')
        s3.Bucket(self.args.s3_bucket_name).put_object(
            Key=img_fname, Body=data)

        # get info on that image
        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)

        # assert the info is correct
        im = np.array(Image.open(local_base_path + img_fname_only))
        assert im_width == im.shape[1]
        assert im_height == im.shape[0]
        assert im_datatype == im.dtype

        s3.Bucket(self.args.s3_bucket_name).delete_objects(
            Delete={'Objects': [{'Key': img_fname}]})

        # closing the boto3 session
        s3.meta.client._endpoint.http_session.close()
        os.remove(ingest_job.get_log_fname())

    def test_get_img_info_uint16_png(self):
        file_format = 'png'
        self.args.file_format = file_format
        ingest_job = IngestJob(self.args)

        z_slice = 0

        img_fname = ingest_job.get_img_fname(z_slice)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, img_fname)

        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)
        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def test_get_img_info_uint64_tif(self):
        file_format = 'tif'
        dtype = 'uint64'
        self.args.file_format = file_format
        self.args.datatype = dtype
        self.args.source_channel = 'def_files'
        ingest_job = IngestJob(self.args)

        z_slice = 0

        img_fname = ingest_job.get_img_fname(z_slice)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        self.args.datatype, self.args.extension, img_fname)

        im_width, im_height, im_datatype = ingest_job.get_img_info(z_slice)
        assert im_width == ingest_job.img_size[0]
        assert im_height == ingest_job.img_size[1]
        assert im_datatype == self.args.datatype
        os.remove(ingest_job.get_log_fname())

    def test_get_img_fname_channel(self):
        self.args.base_filename = 'img_<ch>_<p:4>'
        self.args.base_path = 'local_img_<ch>_test_data\\'

        ingest_job = IngestJob(self.args)

        img_fname = ingest_job.get_img_fname(0)
        assert img_fname == 'local_img_{0}_test_data\\img_{0}_{1:04d}.tif'.format(
            self.args.channel, 0)
        os.remove(ingest_job.get_log_fname())

    def test_read_uint16_img_stack(self):
        ingest_job = IngestJob(self.args)

        # generate some images
        gen_images(ingest_job)

        # load images into memory using ingest_job
        z_slices = range(self.args.z_range[0], self.args.z_range[1])
        im_array = ingest_job.read_img_stack(z_slices)

        # check to make sure each image is equal to each z index in the array
        for z in z_slices:
            img_fname = self.args.base_path + 'img_{:04d}.tif'.format(z)
            with Image.open(img_fname) as im:
                assert np.array_equal(im_array[z, :, :], im)

        del_test_images(ingest_job)
        os.remove(ingest_job.get_log_fname())
