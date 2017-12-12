import os
from argparse import Namespace

import numpy as np
import pytest

from ....repeat_cutouts import Cutout, ingest_cuts, parse_cut_line
from ..boss_resources import BossResParams
from ..ingest_job import IngestJob
from .create_images import del_test_images, gen_images


class TestRepeatCutouts:

    def setup(self):
        pass

    def test_parse_cut_line(self):
        cutout_text = 'Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)\n'
        coll, exp, ch, x, y, z = parse_cut_line(cutout_text)
        cut = Cutout(coll, exp, ch, x, y, z)

        assert cut.collection == coll
        assert cut.experiment == exp
        assert cut.channel == ch
        assert cut.x == x
        assert cut.y == y
        assert cut.z == z

    def test_get_boss_resources(self):
        cutout_text = 'Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)\n'
        coll, exp, ch, x, y, z = parse_cut_line(cutout_text)

        datasource, s3_bucket_name, aws_profile, boss_config_file, base_path, base_filename, extension, z_step, datatype = create_local_ingest_params()
        args = Namespace(
            datasource=datasource,
            collection=coll,
            experiment=exp,
            channel=ch,
            datatype=datatype,
            aws_profile=aws_profile,
            boss_config_file=boss_config_file,
            base_path=base_path,
            base_filename=base_filename,
            extension=extension,
            z_range=[0, 16],
            z_step=z_step,
            warn_missing_files=True
        )
        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=True)

        assert boss_res_params.coll_resource.name == coll
        assert boss_res_params.exp_resource.name == exp
        assert boss_res_params.ch_resource.name == ch

        os.remove(ingest_job.get_log_fname())

    def test_local_ingest_cuts(self):
        cut = create_cutout()
        coll, exp, ch = (cut.collection, cut.experiment, cut.channel)

        datasource, s3_bucket_name, aws_profile, boss_config_file, base_path, base_filename, extension, z_step, datatype = create_local_ingest_params()
        args = Namespace(
            datasource=datasource,
            s3_bucket_name=s3_bucket_name,
            collection=coll,
            experiment=exp,
            channel=ch,
            datatype=datatype,
            aws_profile=aws_profile,
            boss_config_file=boss_config_file,
            base_path=base_path,
            base_filename=base_filename,
            extension=extension,
            z_range=[0, 16],
            z_step=z_step,
            warn_missing_files=True
        )

        ingest_job = IngestJob(args)
        boss_res_params = BossResParams(ingest_job, get_only=True)

        gen_images(ingest_job)

        # ingest the cut
        ingest_cuts([cut], ingest_job, boss_res_params)

        # pull the data from the boss after the new ingest
        data_boss = boss_res_params.rmt.get_cutout(
            boss_res_params.ch_resource, 0, cut.x, cut.y, cut.z)

        # test to make sure it's the same as local file
        z_slices = range(cut.z[0], cut.z[1])

        # loading data locally for comparison
        im_array = ingest_job.read_img_stack(z_slices)
        data_local = im_array[:, cut.y[0]:cut.y[1], cut.x[0]:cut.x[1]]
        assert np.array_equal(data_local, data_boss)

        del_test_images(ingest_job)
        os.remove(ingest_job.get_log_fname())
        os.remove(cut.log_fname)

    def test_iterate_posting_cutouts(self):
        pass


def create_cutout():
    cutout_text = 'Coll: ben_dev, Exp: dev_ingest_4, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)\n'
    coll, exp, ch, x, y, z = parse_cut_line(cutout_text)
    return Cutout(coll, exp, ch, x, y, z)


def create_local_ingest_params():
    source_type = 'local'
    s3_bucket_name = None
    aws_profile = None
    boss_config_file = 'neurodata.cfg'
    data_directory = 'local_img_test_data\\'
    file_name_pattern = 'img_<p:4>'
    img_format = 'tif'
    z_step = 1
    datatype = 'uint16'
    return source_type, s3_bucket_name, aws_profile, boss_config_file, data_directory, file_name_pattern, img_format, z_step, datatype
