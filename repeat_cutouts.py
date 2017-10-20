import argparse
import os
import re

import numpy as np

from boss_resources import BossResParams
from ingest_large_vol import create_s3_res, post_cutout, read_img_stack
from intern.remote.boss import BossRemote
from parse_log import parse_log


class Cutout():
    def __init__(self, coll, exp, ch, x, y, z):
        self.collection = coll
        self.experiment = exp
        self.channel = ch
        self.x = x
        self.y = y
        self.z = z

        self.log_fname = self.gen_log_fname()

    def cutout_string(self):
        return ('Coll: {}, Exp: {}, Ch: {}, x: {}, y: {}, z: {}'.format(
            self.collection, self.experiment, self.channel, self.x, self.y, self.z))

    def gen_log_fname(self):
        return 'ingest_log_{}_{}_{}_repeat.txt'.format(
            self.collection, self.experiment, self.channel)

    def send_msg(self, msg):
        print(msg)
        with open(self.log_fname, 'a') as f:
            f.write(msg + '\n')


class ImgData():
    def __init__(self, im_array, z_rng):
        self.im_data = im_array
        self.z_rng = z_rng
        self.y = np.shape(im_array)[1]
        self.x = np.shape(im_array)[2]


class IngestJob():
    def __init__(self, **kwargs):
        # source_type, s3_bucket_name, aws_profile, boss_config_file, data_directory, file_name_pattern, img_format, z_step
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.rmt = BossRemote(self.boss_config_file)


def parse_cut_line(c_line):
    coll = re.search('Coll: (.+?),', c_line).group(1)
    exp = re.search('Exp: (.+?),', c_line).group(1)
    ch = re.search('Ch: (.+?),', c_line).group(1)
    x = list(map(int, re.search('x: \((.+?)\)', c_line).group(1).split(', ')))
    y = list(map(int, re.search('y: \((.+?)\)', c_line).group(1).split(', ')))
    z = list(map(int, re.search('z: \((.+?)\)', c_line).group(1).split(', ')))

    return coll, exp, ch, x, y, z


def gather_info():
    s = input('Source type (either "local" or "s3"): ')
    if 'local' == s or 's3' == s:
        source_type = s
    else:
        raise TypeError

    if source_type == 's3':
        s = input('s3 bucket name: ')
        s3_bucket_name = s  # not used for 'local' source_type

        s = input('aws profile ("default"): ') or 'default'
        aws_profile = s  # not used for 'local' source_type
    else:
        s3_bucket_name = None
        aws_profile = None

    s = input('BOSS config file path ("neurodata.cfg"): ') or 'neurodata.cfg'
    boss_config_file = s  # location on local system for boss API key

    s = input('data directory (with trailing slash): ')
    # data_directory _with_ trailing slash (doesn't output correct paths on Windows)
    data_directory = s

    # filename without extension (no '.tif')
    # <p:4> indicates the z index of the tif file, with up to 4 leading zeros
    s = input('Filename with  (e.g. "FILENAME<p:4>"): ')
    file_name_pattern = s

    # extension name for images, supported image types are PNG and TIFF
    # extension just needs to match the filename and can be any string (e.g.: ome, tif, png)
    s = input('File format ("tif", "png"): ')
    img_format = s

    # increment of filename numbering (always increment in steps of 1 in the boss, typically will be '1')
    s = input('Z step size ("1"): ') or '1'
    z_step = int(s)

    ingestjob = IngestJob(source_type=source_type, s3_bucket_name=s3_bucket_name, aws_profile=aws_profile, boss_config_file=boss_config_file,
                          data_directory=data_directory, file_name_pattern=file_name_pattern, img_format=img_format, z_step=z_step)
    return ingestjob


def ingest_cuts(cutouts, ingestjob):
    coll, exp, ch = (cutouts[-1].collection,
                     cutouts[-1].experiment, cutouts[-1].channel)

    # going to try to get these things from the resources that already exist on the boss:
    boss_res_params = BossResParams(coll, exp, ch)
    boss_res_params.setup_resources(ingestjob.rmt, get_only=True)

    if ingestjob.source_type is 's3':
        s3_res = create_s3_res(
            boss_res_params, ingestjob.source_type, ingestjob.s3_bucket_name, ingestjob.aws_profile)
    else:
        s3_res = None

    # sort by z, y, x...
    cutouts.sort(key=lambda c: (c.z, c.y, c.x))

    z_extent = [boss_res_params.coord_frame_resource.z_start,
                boss_res_params.coord_frame_resource.z_stop]

    imgdata = None
    for cut in cutouts:
        cut.send_msg(
            'Attempting re-ingest of cutout: {}'.format(cut.cutout_string()))

        # if the data is not in the previous data (same z slice)
        # load the data
        if imgdata is None or imgdata.z_rng != cut.z:
            z_slices = range(cut.z[0], cut.z[1])
            im_array = read_img_stack(boss_res_params, z_slices, ingestjob.file_name_pattern, ingestjob.data_directory,
                                      ingestjob.img_format, s3_res, ingestjob.s3_bucket_name, z_extent, ingestjob.z_step, warn_missing_files=True)
            imgdata = ImgData(im_array, cut.z)

        data = imgdata.im_data[:, cut.y[0]:cut.y[1], cut.x[0]:cut.x[1]]
        data = np.asarray(data, order='C')
        ret_val = post_cutout(boss_res_params, cut.x[0], cut.x[1], cut.y[0],
                              cut.y[1], cut.z[0], cut.z[1], data, attempts=2)
        if ret_val == 0:
            cut.send_msg(
                'Successful re-ingest of cutout: {}'.format(cut.cutout_string()))
        else:
            cut.send_msg(
                'Error: re-ingest of cutout failed: {}'.format(cut.cutout_string()))

    cutouts[-1].send_msg('Finished cutouts for collection {}, experiment {}, channel {}'.format(
        coll, exp, ch))


def get_cutouts(repeatfile):
    cutouts = []

    # load the repeat_cutouts file
    with open(repeatfile) as f:
        log_lines = f.readlines()

    # for each line in the repeat cutouts, extract the coll, experiment, channel, x, y, and z
    for line in log_lines:
        coll, exp, ch, x, y, z = parse_cut_line(line)
        cutouts.append(Cutout(coll, exp, ch, x, y, z))

    return cutouts


def iterate_posting_cutouts(cutouts):
    # separate the cutouts into groupings of shared collections/experiments/channels
    collections = set([cu.collection for cu in cutouts])
    for coll in collections:
        cus_coll = [cu for cu in cutouts if cu.collection == coll]
        experiments = set([cu.experiment for cu in cus_coll])
        for exp in experiments:
            cus_exp = [cu for cu in cus_coll if cu.experiment == exp]
            channels = set([cu.channel for cu in cus_exp])
            for ch in channels:
                cus_ch = [cu for cu in cus_exp if cu.channel == ch]
                if len(cus_ch) > 0:
                    # posts data for cutouts that share a common coll, exp, and ch
                    msg = 'Repeating cutouts for collection {}, experiment {}, channel {}'.format(
                        coll, exp, ch)
                    cus_ch[-1].send_msg(msg)
                    ingestjob = gather_info()
                    ingest_cuts(cus_ch, ingestjob)


def main():
    parser = argparse.ArgumentParser(
        description='Search log file for errors and post the data to the boss')
    parser.add_argument('--logfile', type=str,
                        default=None, help='log file to parse')
    parser.add_argument('--repeatfile', type=str,
                        default='repeat_cutouts.txt', help='log file to parse')
    args = parser.parse_args()

    if args.logfile is not None:
        args.repeatfile = parse_log(args.logfile, args.repeatfile)

    cutouts = get_cutouts(args.repeatfile)

    iterate_posting_cutouts(cutouts)

    print('Finished all failed cutouts, check logs for errors')


if __name__ == '__main__':
    main()
