import re
import unittest

import tifffile as tiff
from PIL import Image

from repeat_cutouts import *


class RepeatCutoutsTest(unittest.TestCase):

    def setUp(self):
        self.rmt = BossRemote('neurodata.cfg')

    def tearDown(self):
        pass

    def test_parse_cut_line(self):
        cutout_text = 'Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)\n'
        coll, exp, ch, x, y, z = parse_cut_line(cutout_text)
        cut = Cutout(coll, exp, ch, x, y, z)

        self.assertEqual(cut.collection, coll)
        self.assertEqual(cut.experiment, exp)
        self.assertEqual(cut.channel, ch)
        self.assertEqual(cut.x, x)
        self.assertEqual(cut.y, y)
        self.assertEqual(cut.z, z)

    def test_get_boss_resources(self):
        cutout_text = 'Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)\n'
        coll, exp, ch, x, y, z = parse_cut_line(cutout_text)
        boss_res_params = BossResParams(coll, exp, ch)
        boss_res_params.setup_resources(self.rmt, get_only=True)

        self.assertEqual(boss_res_params.coll_resource.name, coll)
        self.assertEqual(boss_res_params.exp_resource.name, exp)
        self.assertEqual(boss_res_params.ch_resource.name, ch)

    def test_create_local_IngestJob(self):
        source_type, s3_bucket_name, aws_profile, boss_config_file, data_directory, file_name_pattern, img_format, z_step = create_local_ingest_params()
        input_ingest_job = {'source_type': source_type, 's3_bucket_name': s3_bucket_name, 'aws_profile': aws_profile, 'boss_config_file': boss_config_file,
                            'data_directory': data_directory, 'file_name_pattern': file_name_pattern, 'img_format': img_format, 'z_step': z_step}
        ingestjob = IngestJob(**input_ingest_job)

        ingestjob_attributes = {key: value for key, value in ingestjob.__dict__.items(
        ) if not key.startswith('__') and not callable(key)}

        self.assertTrue(all(item in ingestjob_attributes.items()
                            for item in input_ingest_job.items()))

    def test_local_ingest_cuts(self):
        source_type, s3_bucket_name, aws_profile, boss_config_file, data_directory, file_name_pattern, img_format, z_step = create_local_ingest_params()
        input_ingest_job = {'source_type': source_type, 's3_bucket_name': s3_bucket_name, 'aws_profile': aws_profile, 'boss_config_file': boss_config_file,
                            'data_directory': data_directory, 'file_name_pattern': file_name_pattern, 'img_format': img_format, 'z_step': z_step}
        ingestjob = IngestJob(**input_ingest_job)

        cutouts = [create_cutout()]

        # ideally create the tif files here, not rely on other tests to have created them for you

        # ingest the cut
        ingest_cuts(cutouts, ingestjob)

        # pull the data from the boss
        cut = cutouts[-1]
        coll, exp, ch = (cut.collection, cut.experiment, cut.channel)
        boss_res_params = BossResParams(coll, exp, ch)
        boss_res_params.setup_resources(ingestjob.rmt, get_only=True)
        data_boss = self.rmt.get_cutout(
            boss_res_params.ch_resource, 0, cut.x, cut.y, cut.z)

        # test to make sure it's the same as local file
        z_slices = range(cut.z[0], cut.z[1])
        s3_res = None

        # ideally we'd have our own simpler function to read in the image files
        im_array = read_img_stack(boss_res_params, z_slices, file_name_pattern, data_directory,
                                  img_format, s3_res, s3_bucket_name, cut.z, z_step, warn_missing_files=False)
        data_local = im_array[:, cut.y[0]:cut.y[1], cut.x[0]:cut.x[1]]
        self.assertTrue(np.array_equal(data_local, data_boss))


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
    return source_type, s3_bucket_name, aws_profile, boss_config_file, data_directory, file_name_pattern, img_format, z_step


if __name__ == '__main__':
    unittest.main()
