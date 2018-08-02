'''
Command line program for ingest to boss
Parses arguments and manages the ingest process
'''

import argparse
import platform
import sys
import time
from collections import defaultdict
from datetime import datetime
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

import numpy as np
from PIL import Image

try:
    # for command line usage
    from src.ingest.boss_resources import BossResParams
    from src.ingest.ingest_job import IngestJob
except ImportError:
    # for imports from tests
    from .src.ingest.boss_resources import BossResParams
    from .src.ingest.ingest_job import IngestJob

Image.MAX_IMAGE_PIXELS = None


def read_channel_names(channels_path):
    try:
        channels = []
        with open(channels_path, 'r') as f:
            for line in f:
                channels.append(line.strip('\n'))
        return channels
    except FileNotFoundError:
        raise FileNotFoundError


def post_cutout(boss_res_params, ingest_job, x_rng, y_rng, z_rng, data, attempts=5):
    ch = ingest_job.ch_name
    cutout_msg = 'Coll: {}, Exp: {}, Ch: {}, x: {}, y: {}, z: {}'.format(
        ingest_job.coll_name, ingest_job.exp_name, ch, x_rng, y_rng, z_rng)
    # POST cutout
    for attempt in range(attempts):
        try:
            start_time = time.time()
            boss_res_params.rmt.create_cutout(boss_res_params.ch_resource, ingest_job.res,
                                              x_rng, y_rng, z_rng, data)
            end_time = time.time()
            post_time = end_time - start_time
            msg = '{} POST succeeded in {:.2f} sec. {}'.format(
                get_formatted_datetime(), post_time, cutout_msg)
            ingest_job.send_msg(msg)
        except Exception as e:
            # attempt failed
            ingest_job.send_msg(str(e))
            if attempt != attempts - 1:
                time.sleep(2**(attempt + 1))
        else:
            break
    else:
        # we failed all the attempts - deal with the consequences.
        msg = '{} Error: data upload failed after multiple attempts, skipping. {}'.format(
            get_formatted_datetime(), cutout_msg)
        ingest_job.send_msg(msg, send_slack=True)
        ingest_job.num_POST_failures += 1
        return 1
    return 0


def download_boss_slice(boss_res_params, ingest_job, z_slice, attempts=3):
    im_array_boss = np.zeros([1, ingest_job.img_size[1], ingest_job.img_size[0]],
                             dtype=ingest_job.datatype)

    rmt = boss_res_params.rmt
    stride = 2048

    x_buckets = get_supercube_lims(ingest_job.x_extent, stride=stride)
    y_buckets = get_supercube_lims(ingest_job.y_extent, stride=stride)
    for _, y_slices in y_buckets.items():
        y_rng = [y_slices[0],
                 y_slices[-1]+1]
        for _, x_slices in x_buckets.items():
            x_rng = [x_slices[0],
                     x_slices[-1]+1]
            for attempt in range(attempts):
                try:
                    im_array_boss[0, y_rng[0]-ingest_job.y_extent[0]:y_rng[1]-ingest_job.y_extent[0],
                                  x_rng[0]-ingest_job.x_extent[0]:x_rng[-1]-ingest_job.x_extent[0]] = rmt.get_cutout(
                        boss_res_params.ch_resource,
                        ingest_job.res,
                        x_rng, y_rng, [z_slice, z_slice + 1]
                    )
                except Exception as e:
                    # attempt failed
                    ingest_job.send_msg(str(e))
                    if attempt != attempts - 1:
                        time.sleep(2**(attempt + 1))
                else:
                    break
            else:
                # we failed all the attempts - deal with the consequences.
                msg = '{} Error: download cutout failed after multiple attempts'.format(
                    get_formatted_datetime())
                ingest_job.send_msg(msg, send_slack=True)
    return im_array_boss


def assert_equal(boss_res_params, ingest_job, z_rng):
    ingest_job.send_msg('Checking to make sure data has been POSTed correctly')

    # choose a rand slice:
    rand_slice = np.random.randint(z_rng[0], z_rng[1])

    # load data from Boss
    msg = '{} Getting random z slice from BOSS for comparison'.format(
        get_formatted_datetime())
    ingest_job.send_msg(msg)

    im_array_boss = download_boss_slice(
        boss_res_params, ingest_job, rand_slice + ingest_job.offsets[2])

    msg = '{} Z slice from BOSS downloaded'.format(
        get_formatted_datetime())
    ingest_job.send_msg(msg)

    # load source data (one z slice)
    im_array_local = ingest_job.read_img_stack([rand_slice])

    # assert that cutout from the boss is the same as what was sent
    if np.array_equal(im_array_boss, im_array_local):
        ingest_job.send_msg('Test slice {} in Boss matches file {}'.format(
            rand_slice, ingest_job.get_img_fname(rand_slice)))
        return True
    else:
        ingest_job.send_msg('Test slice {} in Boss does *NOT* match file {}'.format(
            rand_slice, ingest_job.get_img_fname(rand_slice)), send_slack=True)
        return False


def get_supercube_lims(rng, stride=16):
    # stride = height of super cuboid

    first = rng[0]    # inclusive
    last = rng[1]     # exclusive

    buckets = defaultdict(list)
    for z in range(first, last):
        buckets[(z // stride)].append(z)

    return buckets


def get_formatted_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ingest_block(x_slice_key, x_buckets, boss_res_params, ingest_job, y_rng, z_rng, im_array):
    # created for multithreading
    x_slices = x_buckets[x_slice_key]

    x_rng = [x_slices[0], x_slices[-1] + 1]

    data = im_array[:, y_rng[0]-ingest_job.y_extent[0]:y_rng[1]-ingest_job.y_extent[0],
                    x_rng[0]-ingest_job.x_extent[0]:x_rng[1]-ingest_job.x_extent[0]]
    data = np.asarray(data, order='C')

    if np.sum(data) == 0:
        ingest_job.send_msg('{} Block empty for Collection: {}, Experiment: {}, Channel: {} x/y/z: {}/{}/{}, skipping'.format(
            get_formatted_datetime(),
            ingest_job.coll_name, ingest_job.exp_name, ingest_job.ch_name, x_rng, y_rng, z_rng))
        return

    # POST each block to the BOSS
    post_cutout(boss_res_params, ingest_job,
                x_rng, y_rng, z_rng, data, attempts=3)


def per_channel_ingest(args, channel, threads=8):
    args.channel = channel
    ingest_job = IngestJob(args)

    # extract img_size and datatype to check inputs (by actually reading the data)
    # this can take a while, as we actually load in the first image slice,
    # so we should store this first slice so we don't have to load it again when we later read the entire chunk in z
    # we don't do this for render data source because we get the image size and attributes from the render metadata and the # of bits aren't in the metadata or render
    if ingest_job.datasource != 'render':
        im_width, im_height, im_datatype = ingest_job.get_img_info(
            ingest_job.z_range[0])

        # we do this before creating boss resources that could be inaccurate
        try:
            assert ingest_job.img_size[0] == im_width and ingest_job.img_size[
                1] == im_height and ingest_job.datatype == im_datatype
        except AssertionError:
            ingest_job.send_msg('Mismatch between image file and input parameters. Determined image width: {}, height: {}, datatype: {}'.format(
                im_width, im_height, im_datatype))
            raise ValueError('Image attributes do not match arguments')

    # create or get the boss resources for the data
    get_only = not ingest_job.create_resources
    boss_res_params = BossResParams(ingest_job, get_only)

    # we just create the resources, don't do anything else
    if ingest_job.create_resources:
        ingest_job.send_msg('{} Resources set up. Collection: {}, Experiment: {}, Channel: {}'.format(
            get_formatted_datetime(), ingest_job.coll_name, ingest_job.exp_name, ingest_job.ch_name))
        return 0
    else:
        ingest_job.send_msg('{} Starting ingest for Collection: {}, Experiment: {}, Channel: {}, Z: {z[0]},{z[1]}'.format(
            get_formatted_datetime(), ingest_job.coll_name, ingest_job.exp_name, ingest_job.ch_name, z=ingest_job.z_range))

    # we begin the ingest here:
    stride_x = 1024
    stride_y = 1024
    stride_z = 16
    x_buckets = get_supercube_lims(ingest_job.x_extent, stride_x)
    y_buckets = get_supercube_lims(ingest_job.y_extent, stride_y)
    z_buckets = get_supercube_lims(ingest_job.z_range, stride_z)

    pool = ThreadPool(threads)

    # load images files in stacks of 16 at a time into numpy array
    for _, z_slices in z_buckets.items():
        # read images into numpy array
        im_array = ingest_job.read_img_stack(z_slices)
        z_rng = [z_slices[0] - ingest_job.offsets[2],
                 z_slices[-1] + 1 - ingest_job.offsets[2]]

        # slice into np array blocks
        for _, y_slices in y_buckets.items():
            y_rng = [y_slices[0], y_slices[-1] + 1]

            ingest_block_partial = partial(
                ingest_block, x_buckets=x_buckets, boss_res_params=boss_res_params, ingest_job=ingest_job,
                y_rng=y_rng, z_rng=z_rng, im_array=im_array)
            pool.map(ingest_block_partial, x_buckets.keys())

    # checking data posted correctly for an entire z slice
    assert_equal(boss_res_params, ingest_job, ingest_job.z_range)

    ch_link = (
        'http://ndwt.neurodata.io/channel_detail/{}/{}/{}/').format(ingest_job.coll_name, ingest_job.exp_name, ingest_job.ch_name)

    ingest_job.send_msg('{} Finished z slices {} for Collection: {}, Experiment: {}, Channel: {}\nThere were {} read failures and {} POST failures.\nView properties of channel and start downsample job on ndwebtools: {}'.format(
        get_formatted_datetime(),
        ingest_job.z_range, ingest_job.coll_name, ingest_job.exp_name, ingest_job.ch_name,
        ingest_job.num_READ_failures, ingest_job.num_POST_failures, ch_link), send_slack=True)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Copy image z stacks to Boss for a single channel')

    parser.add_argument('--base_path', type=str,
                        help='Directory where image stacks are located (e.g. "/data/images/"')
    parser.add_argument('--base_filename', type=str,
                        help='Base filename with z values specified "ch1_<>" or w/ leading zeros "ch1_<p:4>"')
    parser.add_argument('--extension', type=str, help='Extension (tif(f)/png)')
    parser.add_argument('--datasource', type=str, default='local',
                        help='Location of files, either "local", "s3", or "render"')
    parser.add_argument('--collection', type=str, help='Collection')
    parser.add_argument('--experiment', type=str, help='Experiment')

    parser.add_argument('--channel', type=str, help='Channel')
    parser.add_argument('--channels_list_file', type=str,
                        help='Path to a file with list of channels separated into separate lines')

    parser.add_argument('--voxel_size', type=float,
                        nargs=3, help='Voxel size in x y z')
    parser.add_argument('--voxel_unit', type=str,
                        help='Voxel unit, nanometers/micrometers/millimeters/centimeters')
    parser.add_argument('--datatype', type=str,
                        help='Data type (uint8/uint16/[uint64-annotations])')
    parser.add_argument('--source_channel', type=str,
                        help='Name of reference channel for annotation channels')
    parser.add_argument('--res', type=int, default=0,
                        help='Resolution to copy (default = 0)')
    parser.add_argument('--z_step', type=int, default=1,
                        help='Z step size for input files, default 1 (on Boss, z step is always 1, and z_rng and img_size for z should both assume increments of 1)')
    parser.add_argument('--create_resources', action='store_true',
                        help='Creates the boss resources and exits')

    parser.add_argument('--x_extent', type=int, nargs=2,
                        help='Volume extent in x (width)')
    parser.add_argument('--y_extent', type=int, nargs=2,
                        help='Volume extent in y (height)')
    parser.add_argument('--z_extent', type=int, nargs=2,
                        help='Volume extent in z (slices/images)')
    parser.add_argument('--offset_extents', action='store_true',
                        help='Offset any negative extents to start at zero and store the original values as BOSS metadata')
    parser.add_argument('--forced_offsets', type=int, nargs=3,
                        help='Offset negative extents by a certain value in x/y/z')
    parser.add_argument('--coord_frame_x_extent', type=int, nargs=2,
                        help='Force coordinate frame to be of a partical x extent (must contain data after any offsets)')
    parser.add_argument('--coord_frame_y_extent', type=int, nargs=2,
                        help='Force coordinate frame to be of a partical y extent (must contain data after any offsets)')
    parser.add_argument('--coord_frame_z_extent', type=int, nargs=2,
                        help='Force coordinate frame to be of a partical z extent (must contain data after any offsets)')

    parser.add_argument('--z_range', type=int, nargs=2,
                        help='Z slices to ingest: start (inclusive) end (exclusive)')

    parser.add_argument('--warn_missing_files', action='store_true',
                        help='Warn on missing files instead of failing')

    parser.add_argument('--s3_bucket_name', type=str,
                        help='S3 bucket name')
    parser.add_argument('--aws_profile', type=str, default='default',
                        help='Name of profile in .aws/credentials file (default = default)')

    parser.add_argument('--boss_config_file', type=str, default='neurodata.cfg',
                        help='Path and filename for Boss config (config file w/ server and API Key)')

    parser.add_argument('--slack_token_file', type=str, default='slack_token',
                        help='Path & filename for slack token (key only)')
    parser.add_argument('--slack_usr', type=str,
                        help='User to send slack message to (e.g. USERNAME)')

    parser.add_argument('--render_owner', type=str,
                        help='Name of owner in render')
    parser.add_argument('--render_project', type=str,
                        help='Name of project in render')
    parser.add_argument('--render_stack', type=str,
                        help='Name of stack in render')
    parser.add_argument('--render_channel', type=str,
                        help='Name of channel in render (some stacks don''t have channels so this would not be entered)')
    parser.add_argument('--render_baseURL', type=str,
                        help='Base URL for render instance (https://render-dev-eric.neurodata.io/render-ws/v1/)')
    parser.add_argument('--render_scale', type=float,
                        help='Scale the data imported from render by this factor')
    parser.add_argument('--render_window', type=int, nargs=2,
                        help='Window used on 16bit -> 8 bit data conversion')

    parser.add_argument('--limit_x', type=int, nargs=2,
                        help='Enforced limit in x (down to level of coord frame) to get & post data')
    parser.add_argument('--limit_y', type=int, nargs=2,
                        help='Enforced limit in y (down to level of coord frame) to get & post data')
    parser.add_argument('--limit_z', type=int, nargs=2,
                        help='Enforced limit in z (down to level of coord frame) to get & post data')

    args = parser.parse_args()

    # if not 64 bit python raise an error
    assert platform.architecture()[0] == '64bit'

    # if not python 3.5 or greater, raise an error
    assert sys.version_info >= (3, 5)

    # Iterate through channels if channels path specified
    if args.channels_list_file is not None:
        channels = read_channel_names(args.channels_list_file)
    else:
        channels = [args.channel]

    for channel in channels:
        per_channel_ingest(args, channel)


if __name__ == '__main__':
    main()
