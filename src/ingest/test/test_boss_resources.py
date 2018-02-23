import ast
import os
from argparse import Namespace
from datetime import datetime

import pytest
from requests import HTTPError

from ..boss_resources import BossResParams
from ..ingest_job import IngestJob

BOSS_URL = 'https://api.boss.neurodata.io/latest/'


class TestBossResources:

    def setup(self):
        pass

    def test_get_boss_res_params_just_names(self):
        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_4',
            channel='def_files',
            boss_config_file='neurodata.cfg',
            x_extent=[0, 1000],
            y_extent=[0, 1024],
            z_extent=[0, 100])

        ingest_job = IngestJob(args)

        boss_res_params = BossResParams(ingest_job, get_only=True)

        assert boss_res_params.coll_resource.name == args.collection
        assert boss_res_params.exp_resource.name == args.experiment
        assert boss_res_params.ch_resource.name == args.channel
        assert boss_res_params.exp_resource.hierarchy_method == 'isotropic'
        assert ingest_job.voxel_size == [1, 1, 1]
        assert ingest_job.voxel_unit == 'micrometers'
        assert ingest_job.offsets == [0, 0, 0]
        assert ingest_job.datatype == 'uint16'
        assert ingest_job.boss_datatype == 'uint16'
        assert ingest_job.res == 0
        assert ingest_job.extension is None
        assert ingest_job.z_step is None

        os.remove(ingest_job.get_log_fname())

    def test_create_boss_res(self):
        now = datetime.now()

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_4' + now.strftime("%Y%m%d-%H%M%S"),
            channel='def_files_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            voxel_size=[1, 5, 1],
            voxel_unit='nanometers',
            datatype='uint16',
            res=0,
            x_extent=[0, 1000],
            y_extent=[0, 1024],
            z_extent=[0, 100])

        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=False)

        assert boss_res_params.ch_resource.name == args.channel
        assert boss_res_params.exp_resource.hierarchy_method == 'anisotropic'
        assert ingest_job.x_extent == args.x_extent
        assert ingest_job.y_extent == args.y_extent
        assert ingest_job.z_extent == args.z_extent
        assert ingest_job.voxel_size == args.voxel_size
        assert ingest_job.voxel_unit == args.voxel_unit
        assert ingest_job.img_size == [1000, 1024, 100]
        assert ingest_job.offsets == [0, 0, 0]
        assert ingest_job.boss_datatype == 'uint16'
        assert ingest_job.res == 0
        assert ingest_job.extension is None
        assert ingest_job.z_step is None

        os.remove(ingest_job.get_log_fname())
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def test_create_boss_res_offsets(self):
        now = datetime.now()

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_neg' + now.strftime("%Y%m%d-%H%M%S"),
            channel='def_files_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            voxel_size=[1, 5, 1],
            voxel_unit='nanometers',
            datatype='uint16',
            res=0,
            x_extent=[-500, 500],
            y_extent=[0, 1024],
            z_extent=[200, 300],
            offset_extents=True)

        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=False)

        assert boss_res_params.coord_frame_resource.z_start == 200
        assert boss_res_params.coord_frame_resource.z_stop == 300
        assert boss_res_params.coord_frame_resource.x_start == 0
        assert boss_res_params.coord_frame_resource.x_stop == 1000

        assert ingest_job.offsets == [500, 0, 0]

        # testing to make sure offsets were recorded properly
        exp_res = boss_res_params.exp_resource
        boss_offsets_dict = boss_res_params.rmt.get_metadata(
            exp_res, ['offsets'])
        boss_offsets = ast.literal_eval(boss_offsets_dict['offsets'])
        assert boss_offsets == [500, 0, 0]

        os.remove(ingest_job.get_log_fname())
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def test_create_boss_res_forced_offsets(self):
        now = datetime.now()

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_neg' + now.strftime("%Y%m%d-%H%M%S"),
            channel='def_files_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            voxel_size=[1, 5, 1],
            voxel_unit='nanometers',
            datatype='uint16',
            res=0,
            x_extent=[-500, 500],
            y_extent=[0, 1024],
            z_extent=[200, 300],
            forced_offsets=[600, 500, 400])

        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=False)

        assert boss_res_params.coord_frame_resource.z_start == 200+400
        assert boss_res_params.coord_frame_resource.z_stop == 300+400
        assert boss_res_params.coord_frame_resource.x_start == -500+600
        assert boss_res_params.coord_frame_resource.x_stop == 500+600

        assert ingest_job.offsets == [600, 500, 400]

        # testing to make sure offsets were recorded properly
        exp_res = boss_res_params.exp_resource
        boss_offsets_dict = boss_res_params.rmt.get_metadata(
            exp_res, ['offsets'])
        boss_offsets = ast.literal_eval(boss_offsets_dict['offsets'])
        assert boss_offsets == [600, 500, 400]

        os.remove(ingest_job.get_log_fname())
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def test_create_boss_res_specified_coord_frame(self):
        now = datetime.now()

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_neg' + now.strftime("%Y%m%d-%H%M%S"),
            channel='def_files_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            voxel_size=[1, 5, 1],
            voxel_unit='nanometers',
            datatype='uint16',
            res=0,
            x_extent=[100, 1100],
            y_extent=[0, 1024],
            z_extent=[200, 300],
            coord_frame_x_extent=[0, 2000])

        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=False)

        assert boss_res_params.coord_frame_resource.z_start == 200
        assert boss_res_params.coord_frame_resource.z_stop == 300
        assert boss_res_params.coord_frame_resource.x_start == 0
        assert boss_res_params.coord_frame_resource.x_stop == 2000

        assert ingest_job.x_extent == [100, 1100]
        assert ingest_job.y_extent == [0, 1024]

        assert ingest_job.offsets == [0, 0, 0]

        os.remove(ingest_job.get_log_fname())
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
        boss_res_params.rmt.delete_project(boss_res_params.exp_resource)

    def test_get_boss_res_wrong_img_size(self):
        now = datetime.now()

        x_extent = [0, 2000]
        y_extent = [0, 1000]
        z_extent = [0, 50]
        voxel_size = [1, 5, 1]
        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_4' + now.strftime("%Y%m%d-%H%M%S"),
            channel='def_files_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            voxel_size=voxel_size,
            voxel_unit='nanometers',
            datatype='uint16',
            res=0,
            x_extent=x_extent,
            y_extent=y_extent,
            z_extent=z_extent)

        ingest_job = IngestJob(args)
        with pytest.raises(HTTPError):
            boss_res_params = BossResParams(ingest_job, get_only=True)

        os.remove(ingest_job.get_log_fname())

    def test_get_boss_annotation_channel(self):
        datatype = 'uint64'

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_4',
            channel='def_files_annot',
            boss_config_file='neurodata.cfg',
            source_channel='def_files',
            x_extent=[0, 1000],
            y_extent=[0, 1024],
            z_extent=[0, 100]
        )

        ingest_job = IngestJob(args)

        boss_res_params = BossResParams(ingest_job, get_only=True)

        assert ingest_job.ch_name == boss_res_params.ch_resource.name
        assert ingest_job.boss_datatype == datatype
        assert ingest_job.ch_type == 'annotation'
        assert boss_res_params.ch_resource.type == 'annotation'
        assert boss_res_params.ch_resource.sources == [args.source_channel]

        os.remove(ingest_job.get_log_fname())

    def test_create_boss_annotation_channel(self):
        now = datetime.now()

        datatype = 'uint64'

        args = Namespace(
            datasource='local',
            collection='ben_dev',
            experiment='dev_ingest_4',
            channel='def_files_annotation_' + now.strftime("%Y%m%d-%H%M%S"),
            boss_config_file='neurodata.cfg',
            source_channel='def_files',
            voxel_size=[1, 1, 1],
            voxel_unit='micrometers',
            datatype=datatype,
            res=0,
            x_extent=[0, 1000],
            y_extent=[0, 1024],
            z_extent=[0, 100])

        ingest_job = IngestJob(args)

        boss_res_params = BossResParams(ingest_job, get_only=False)

        assert ingest_job.ch_name == boss_res_params.ch_resource.name
        assert ingest_job.boss_datatype == datatype
        assert ingest_job.ch_type == 'annotation'
        assert boss_res_params.ch_resource.type == 'annotation'
        assert boss_res_params.ch_resource.sources == [args.source_channel]

        os.remove(ingest_job.get_log_fname())
        boss_res_params.rmt.delete_project(boss_res_params.ch_resource)
