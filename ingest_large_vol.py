from datetime import datetime
import time
import os
import re
from collections import defaultdict
import argparse

from intern.remote.boss import BossRemote
from boss_resources import setup_boss_resources

import boto3

import numpy as np

from PIL import Image

from slacker import Slacker

import tailer

def create_slack_session(slack_token_file):
    #generate token here: https://api.slack.com/custom-integrations/legacy-tokens, put in file in same directory -> "slack_token"
    try:
        with open(slack_token_file,'r') as s:
            token = s.readline().split("\n")
        slack = Slacker(token[0])
        return slack
    except FileNotFoundError:
        print('Slack token not found, create slack_token file for sending slack messages')
        return None

def send_msg(msg, slack=None, slack_usr=None):
    print(msg)
    with open('log.txt', 'a') as f:
        f.write(msg + '\n')
    if slack is not None and slack_usr is not None:
        slack.chat.post_message('@'+slack_usr, msg, username='local_ingest.py')
        content = tailer.tail(open('log.txt'), 10)
        slack.files.upload(content='\n'.join(content), channels='@'+slack_usr,
                           title=get_formatted_datetime() + '_tail_of_log')

def get_img_fname(base_fname, base_path, extension, z_index, z_rng, z_step):
    if z_index >= z_rng[1]:
        raise IndexError("Z-index out of range")

    # return glob.glob(img_path)
    matches = re.findall('<(p:\d+)?>', base_fname)
    for m in matches:
        if m:
            # There is zero padding
            z_str = str(z_index*z_step).zfill(int(m.split(':')[1]))
        else:
            z_str = str(z_index*z_step)
        base_fname = base_fname.replace("<{}>".format(m), z_str)

    # prepend root, append extension
    return os.path.join(base_path, "{}.{}".format(base_fname, extension))

def get_img_info(img_fname, s3_res=None, s3_bucket_name=None):
    im = load_img(img_fname, s3_res, s3_bucket_name)

    width = im.width
    height = im.height
    im_mode = im.tile[0][3]
    if isinstance(im_mode, (tuple)): #tiff files
        im_mode = im_mode[0]
    im_dtype = im_mode[0:4]
    if im_dtype == 'I;16':
        datatype = 'uint16'
    elif im_dtype == 'L':
        datatype = 'uint8'
    return (width, height, datatype)

def load_img(img_fname, s3_res=None, s3_bucket_name=None, warn_missing_files=False):
    if s3_res is None or s3_bucket_name is None:
        if not os.path.isfile(img_fname):
            msg = 'File not found: {}'.format(img_fname)
            if warn_missing_files:
                send_msg(msg)
                return None
            else:
                raise IOError(msg)
        im = Image.open(img_fname)
    else:
        #download the file from s3
        try:
            obj = s3_res.Object(s3_bucket_name, img_fname)
            im = Image.open(obj.get()['Body'])
        except Exception as e:
            msg = 'File not found: {}'.format(img_fname)
            if warn_missing_files:
                send_msg(msg)
                return None
            else:
                raise IOError(msg)
    return im

def read_img_stack(img_size, datatype, z_slices, base_fname, base_path, extension, s3_res, s3_bucket_name, z_rng, z_step, warn_missing_files=False):
    send_msg('{} Reading image data (z range: {}:{})'.format(
        get_formatted_datetime(), z_slices[0], z_slices[-1]+1))

    im_array = np.zeros((len(z_slices), img_size[1], img_size[0]), dtype=datatype, order='C')
    for idx, z_slice in enumerate(z_slices):
        img_fname = get_img_fname(base_fname, base_path, extension, z_slice, z_rng, z_step)
        im = load_img(img_fname, s3_res, s3_bucket_name, warn_missing_files)
        if im is None and warn_missing_files:
            continue
        im_array[idx, :, :] = np.array(im)
    
    send_msg('{} Finished reading image data'.format(get_formatted_datetime()))
    return im_array

def post_cutout(rmt, coll_name, exp_name, channel_resource, resolution, st_x, sp_x, st_y, sp_y, st_z, sp_z, data, attempts=5, slack=None, slack_usr=None):
    ch = channel_resource.name
    cutout_msg = 'Coll: {}, Exp: {}, Ch: {}, x: {}, y: {}, z: {}'.format(
        coll_name, exp_name, ch, (st_x, sp_x), (st_y, sp_y), (st_z, sp_z))
    #POST cutout
    for attempt in range(attempts-1):
        try:
            start_time = time.time()
            rmt.create_cutout(channel_resource, resolution, (st_x, sp_x), (st_y, sp_y), (st_z, sp_z), data)
            end_time = time.time()
            post_time = end_time - start_time
            msg = '{} POST succeeded in {:.2f} sec. '.format(
                get_formatted_datetime(), post_time) + cutout_msg
            send_msg(msg)
        except Exception as e:
            #attempt failed
            send_msg(str(e))
            if attempt != attempts-1:
                time.sleep(2**(attempt+1))
        else:
            break
    else:
        # we failed all the attempts - deal with the consequences.
        msg = '{} Error: data upload failed after multiple attempts, skipping. '.format(
            get_formatted_datetime()) + cutout_msg
        send_msg(msg, slack, slack_usr)

def assert_equal(rmt, z_rng, channel_resource, resolution, img_size, datatype, base_fname, base_path, extension, s3_res, s3_bucket_name, z_step, slack, slack_usr):
    send_msg('Checking to make sure data has been POSTed correctly')

    #choose a rand slice:
    rand_slice = np.random.randint(z_rng[0], z_rng[1])
    
    #load source data (one z slice)
    im_array_local = read_img_stack(img_size, datatype, [rand_slice], base_fname, base_path, extension, s3_res, s3_bucket_name, z_rng, z_step, warn_missing_files=True)
    
    #assemble data from Boss
    im_array_boss = np.zeros(np.shape(im_array_local), dtype=type(im_array_local[0,0]))

    xM = np.shape(im_array_local)[1]
    yM = np.shape(im_array_local)[2]
    stride = 512
    attempts = 3
    for xi in range(0, xM, stride):
        xi_stop = xi+stride
        if xi_stop > xM:
            xi_stop = xM
        for yi in range(0, yM, stride):
            yi_stop = yi+stride
            if yi_stop > yM:
                yi_stop = yM
            for attempt in range(attempts-1):
                try:
                    im_array_boss[0, xi:xi_stop, yi:yi_stop] = rmt.get_cutout(
                        channel_resource, resolution, [yi, yi_stop], [xi, xi_stop], [rand_slice, rand_slice+1]
                    )
                except Exception as e:
                    #attempt failed
                    send_msg(str(e))
                    if attempt != attempts-1:
                        time.sleep(2**(attempt+1))
                else:
                    break
            else:
                # we failed all the attempts - deal with the consequences.
                msg = '{} Error: download cutout failed after multiple attempts'.format(
                    get_formatted_datetime())
                send_msg(msg, slack, slack_usr)

    # assert that cutout from the boss is the same as what was sent
    if np.array_equal(im_array_boss, im_array_local):
        send_msg('Test slice {} in Boss matches file {}'.format(
            rand_slice, get_img_fname(base_fname, base_path, extension, rand_slice, z_rng, z_step)))
    else:
        send_msg('Test slice {} in Boss does *NOT* match file {}'.format(
            rand_slice, get_img_fname(base_fname, base_path, extension, rand_slice, z_rng, z_step)), 
            slack, slack_usr)

def get_supercube_zs(z_rng):
    first = z_rng[0]    # inclusive
    last = z_rng[1]     # exclusive
    stride = 16         # height of super cuboid

    buckets = defaultdict(list)
    for z in range(first, last):
        buckets[(z // stride)].append(z)

    return buckets

def get_formatted_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    parser = argparse.ArgumentParser(description='Copy image z stacks to Boss for a single channel')

    parser.add_argument('--base_path', type=str, help='Directory where image stacks are located (e.g. "/data/images/"')
    parser.add_argument('--base_filename', type=str, help='Base filename with z values specified "ch1_<>" or w/ leading zeros "ch1_<p:4>"')
    parser.add_argument('--extension', type = str, help='Extension (tif(f)/png)')
    parser.add_argument('--datasource', type=str, default='local', help='Location of files, either "local" or "s3"')
    parser.add_argument('--collection', type=str, help='Collection')
    parser.add_argument('--experiment', type=str, help='Experiment')
    parser.add_argument('--channel', type=str, help='Channel')
    parser.add_argument('--voxel_size', type=float, nargs=3, help='Voxel size in x y z')
    parser.add_argument('--voxel_unit', type=str, help='Voxel unit, nanometers/micrometers/millimeters/centimeters')
    parser.add_argument('--datatype', type=str, help='Data type (uint8/uint16)')
    parser.add_argument('--img_size', type=int, nargs=3, help='Volume extent in x (width) y (height) and z (slices/images)')
    parser.add_argument('--z_range', type=int, nargs=2, help='Z slices to ingest: start (inclusive) end (exclusive)')
    parser.add_argument('--res', type=int, default=0, help='Resolution to copy (default = 0)')
    parser.add_argument('--warn_missing_files', action='store_true', help='Warn on missing files instead of failing')
    parser.add_argument('--slack_usr', type=str, default=None, help='User to send slack message to (e.g. USERNAME)')
    parser.add_argument('--s3_bucket_name', type=str, default=None, help='S3 bucket name')
    parser.add_argument('--boss_config_file', type=str, default='neurodata.cfg', help='Path and filename for Boss config (config file w/ server and API Key)')
    parser.add_argument('--slack_token_file', type=str, default='slack_token', help='Path & filename for slack token (key only)')
    parser.add_argument('--z_step', type=int, default=1, help='Z step size for input files, default 1 (on Boss, z step is always 1, and z_rng and img_size for z should both assume increments of 1)')

    args = parser.parse_args()

    base_path = args.base_path
    base_fname = args.base_filename
    extension = args.extension
    datasource = args.datasource
    coll_name = args.collection
    exp_name = args.experiment
    ch_name = args.channel
    voxel_size = args.voxel_size
    voxel_unit = args.voxel_unit
    dtype = args.datatype
    img_size = args.img_size #list of integers, x & y & z
    z_rng = args.z_range
    res = args.res
    warn_missing_files = args.warn_missing_files
    slack_usr = args.slack_usr
    s3_bucket_name = args.s3_bucket_name
    boss_config_file = args.boss_config_file
    slack_token_file = args.slack_token_file
    z_step = args.z_step

    if datasource == 's3':
        if s3_bucket_name is None:
            raise ValueError('s3 bucket not defined but s3 datasource chosen')
        try:
            s3_res = boto3.resource('s3')
        except ValueError:
            raise ValueError('AWS credentials not set up?')
    else:
        if s3_bucket_name is not None:
            send_msg('s3 bucket name input but source is local')
        s3_res = None

    #extract img_size and datatype to double check
    first_fname = get_img_fname(base_fname, base_path, extension, z_rng[0], z_rng, z_step)
    im_width, im_height, im_datatype = get_img_info(first_fname, s3_res, s3_bucket_name)
    if img_size[0] != im_width or img_size[1] != im_height or dtype != im_datatype:
        send_msg('Mismatch between image file and input parameters. Determined image width: {}, height: {}, datatype: {}'.format(
            im_width, im_height, im_datatype))
        raise ValueError('Image attributes do not match arguments')

    #create a session for the BOSS using intern
    rmt = BossRemote(boss_config_file)

    #creating the slack session
    slack = create_slack_session(slack_token_file)

    #create or get the boss resources for the data
    coll, _, exp, ch = setup_boss_resources(rmt, coll_name, exp_name, ch_name, voxel_size, voxel_unit, dtype, res, img_size)
    send_msg('Resources set up. Collection: {}, Experiment: {}, Channel: {}'.format(coll.name, exp.name, ch.name))
    

    stride_x = 1024
    stride_y = 1024
    z_buckets = get_supercube_zs(z_rng)
    #load images files in stacks of 16 at a time into numpy array
    for _, z_slices in z_buckets.items():
        #read images into numpy array
        im_array = read_img_stack(img_size, dtype, z_slices, base_fname, base_path, extension, 
                                  s3_res, s3_bucket_name, z_rng, z_step, warn_missing_files)

        # slice into np array blocks
        for st_x in range(0, img_size[0], stride_x):
            sp_x = st_x + stride_x
            if sp_x > img_size[0]: 
                sp_x = img_size[0]

            for st_y in range(0, img_size[1], stride_y):
                sp_y = st_y + stride_y
                if sp_y > img_size[1]: 
                    sp_y = img_size[1]
                data = im_array[:, st_y:sp_y, st_x:sp_x]
                data = np.asarray(data, order='C')
                
                # POST each block to the BOSS
                post_cutout(rmt, coll.name, exp.name, ch, res, st_x, sp_x, st_y, sp_y, z_slices[0], z_slices[-1] + 1,
                            data, attempts=3)

    #checking data posted correctly
    assert_equal(rmt, z_rng, ch, res, img_size, dtype, base_fname, base_path, extension, s3_res, s3_bucket_name, z_step, slack, slack_usr)
    
    ndviz_link = ("https://viz-dev.boss.neurodata.io/#!{{'layers':{{'{}':{{'type':'image'_'source':'"
                "boss://https://api.boss.neurodata.io/{}/{}/{}'}}}}_'navigation':{{'pose':{{'position':"
                "{{'voxelSize':[{}_{}_{}]_'voxelCoordinates':[{}_{}_{}]}}}}_'zoomFactor':70}}}}"
                ).format(ch_name, coll_name, exp_name, ch_name, voxel_size[0], voxel_size[1], voxel_size[2], 0, 0, z_rng[0])

    send_msg('{} Finished z slices {} for Collection: {}, Experiment: {}, Channel: {}\n{}'.format(
        get_formatted_datetime(), z_rng, coll.name, exp.name, ch.name, ndviz_link
        ), slack, slack_usr)

if __name__ == '__main__':
    main()
