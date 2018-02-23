'''
Class for each ingest job (per channel)
Stores all the info for each ingest job
'''

import io
import os
import re
import time
from datetime import datetime

import boto3
import numpy as np
import tailer
import tifffile
from PIL import Image
from slacker import Slacker

try:
    from render_resource import renderResource
except ImportError:
    from .render_resource import renderResource


class IngestJob:
    def __init__(self, args_namespace):
        # args is a Namespace (argparse)

        # convert to dict in order to use the get method (returns None if not present in dict)
        args = vars(args_namespace)

        self.datasource = args.get('datasource')

        self.slack_usr = args.get('slack_usr')
        self.s3_bucket_name = args.get('s3_bucket_name')
        self.create_resources = args.get('create_resources')
        self.coll_name = args.get('collection')
        self.exp_name = args.get('experiment')
        self.ch_name = args.get('channel')
        self.datatype = args.get('datatype')
        self.warn_missing_files = args.get('warn_missing_files')
        self.z_range = args.get('z_range')

        self.limit_x = args.get('limit_x')
        self.limit_y = args.get('limit_y')
        self.limit_z = args.get('limit_z')

        self.voxel_size = args.get('voxel_size')
        self.voxel_unit = args.get('voxel_unit')
        self.res = args.get('res')
        self.source_channel = args.get('source_channel')

        if self.source_channel is not None:
            self.boss_datatype = 'uint64'  # force boss datatype to uint64 for annotations
            self.ch_type = 'annotation'
        else:
            # image channels don't have a source
            try:
                assert self.datatype == 'uint8' or self.datatype == 'uint16' or self.datatype is None
            except AssertionError:
                raise ValueError(
                    'image channels in the BOSS only support uint8 or uint16')
            self.boss_datatype = self.datatype
            self.ch_type = 'image'

        if self.datasource == 'render':
            # create a render resource which will populate some of the variables
            render_owner = args.get('render_owner')
            render_project = args.get('render_project')
            render_stack = args.get('render_stack')
            render_channel = args.get('render_channel')
            render_baseURL = args.get('render_baseURL')

            render_scale = args.get('render_scale')
            self.render_window = args.get('render_window')

            # create the render object in order to get the xyz extents
            self.render_obj = renderResource(render_owner, render_project, render_stack, render_baseURL, self.datatype,
                                             channel=render_channel, scale=render_scale, limit_x=self.limit_x, limit_y=self.limit_y, limit_z=self.limit_z)
            self.x_extent = self.render_obj.x_rng
            self.y_extent = self.render_obj.y_rng
            self.z_extent = self.render_obj.z_rng

            self.z_step = 1
            # render resource is set to return PNGs
            self.extension = 'png'
            if self.z_range is None:  # if the user isn't specifying the z range for ingest, we just get the entire extent
                self.z_range = self.z_extent

        # otherwise it's image data
        elif self.datasource == 's3' or self.datasource == 'local':
            self.base_fname = args.get('base_filename')
            self.base_path = args.get('base_path')
            self.extension = args.get('extension')
            self.x_extent = args.get('x_extent')
            self.y_extent = args.get('y_extent')
            self.z_extent = args.get('z_extent')
            self.validate_xyz_limits()
            self.apply_limits()
            self.z_step = args.get('z_step')

        # initialize offset to zero (x,y,z)
        self.offsets = [0, 0, 0]
        self.forced_offsets = args.get('forced_offsets')
        # if user specified, we calculate the offset
        if args.get('offset_extents') or self.forced_offsets is not None:
            # calculate and store offsets
            self.offsets = self.calc_offsets()

            # create new extents
            self.offset_extents()

        self.set_img_size()

        self.coord_frame_x_extent = args.get('coord_frame_x_extent')
        self.coord_frame_y_extent = args.get('coord_frame_y_extent')
        self.coord_frame_z_extent = args.get('coord_frame_z_extent')
        self.set_coord_frame_extents()
        self.validate_coord_frames()

        # creating the slack session
        self.slack_obj = self.create_slack_session(
            args.get('slack_token_file'))

        if self.datasource == 's3':
            self.s3_res = self.create_s3_res(
                aws_profile=args.get('aws_profile'))
        else:
            self.s3_res = None

        self.boss_config_file = args.get('boss_config_file')

        self.num_READ_failures = 0
        self.num_POST_failures = 0

        # Document the arguments passed
        self.send_msg('{} Command parameters used: {}'.format(
            get_formatted_datetime(), args))

    def apply_limits(self):
        if self.limit_x is not None:
            self.x_rng_unscaled = self.limit_x
        if self.limit_y is not None:
            self.y_rng_unscaled = self.limit_y
        if self.limit_z is not None:
            self.z_rng = self.limit_z

    def validate_xyz_limits(self):
        validate_limit(self.x_extent, self.limit_x)
        validate_limit(self.y_extent, self.limit_y)
        validate_limit(self.z_extent, self.limit_z)

        # z range is a limit itself - we check we aren't going over our limit with z_range
        validate_limit(self.limit_z, self.z_range)

    def validate_coord_frames(self):
        coord_extents = [self.coord_frame_x_extent,
                         self.coord_frame_y_extent,
                         self.coord_frame_z_extent]
        extents = [self.x_extent, self.y_extent, self.z_extent]

        if None in extents or None in coord_extents:
            raise ValueError

        for coord, ext in zip(coord_extents, extents):
            if coord[0] > ext[0] or coord[1] < ext[1]:
                raise ValueError

    def create_slack_session(self, slack_token_file):
        if slack_token_file is None:
            return None

        if self.slack_usr is None:
            self.send_msg(
                'Slack user not specified, Slack messages will not be sent')
            return None

        # generate token here: https://api.slack.com/custom-integrations/legacy-tokens, put in file in same directory -> "slack_token"
        try:
            with open(slack_token_file, 'r') as s:
                token = s.readline().split("\n")
            return Slacker(token[0])
        except FileNotFoundError:
            self.send_msg('Slack token file not found: {}, create slack_token file for sending Slack messages'.format(
                slack_token_file))
            return None

    def create_s3_res(self, aws_profile='default'):
        # initiating the S3 resource:
        if self.datasource == 's3':
            if self.s3_bucket_name is None:
                raise ValueError(
                    's3 bucket not defined but s3 datasource chosen')
            try:
                s3_session = boto3.session.Session(profile_name=aws_profile)
                s3_res = s3_session.resource('s3')
            except ValueError:
                raise ValueError('AWS credentials not set up?')
        else:
            if self.s3_bucket_name is not None:
                self.send_msg('s3 bucket name input but source is local')
            s3_res = None
        return s3_res

    def get_log_fname(self):
        return '_'.join(('ingest_log', self.coll_name, self.exp_name, self.ch_name)) + '.txt'

    def send_msg(self, msg, send_slack=False):
        logfile = self.get_log_fname()

        print(msg)
        with open(logfile, 'a') as f:
            f.write(msg + '\n')
        if send_slack and self.slack_obj is not None:
            self.slack_obj.chat.post_message(
                '@' + self.slack_usr, msg, username='local_ingest.py')
            content = tailer.tail(open(logfile), 10)
            self.slack_obj.files.upload(content='\n'.join(
                content), channels='@' + self.slack_usr, title=get_formatted_datetime() + '_tail_of_log')

    def calc_offsets(self):
        if self.forced_offsets is not None:
            return self.forced_offsets

        offsets = []
        for extent in [self.x_extent, self.y_extent, self.z_extent]:
            offset = 0
            if extent[0] < 0:
                offset = abs(extent[0])
            offsets.append(offset)
        return offsets

    def offset_extents(self):
        self.x_extent, self.y_extent, self.z_extent = [
            [ext[0] + off, ext[1] + off] for ext, off in zip([self.x_extent, self.y_extent, self.z_extent], self.offsets)]

    def set_coord_frame_extents(self):
        if self.coord_frame_x_extent is None:
            self.coord_frame_x_extent = self.x_extent
        if self.coord_frame_y_extent is None:
            self.coord_frame_y_extent = self.y_extent
        if self.coord_frame_z_extent is None:
            self.coord_frame_z_extent = self.z_extent

    def set_img_size(self):
        # validation that xyz extents are not negative
        # they could be None if just trying to get the Boss resource (not set it)
        if all([a is not None for a in [self.x_extent, self.y_extent, self.z_extent]]):
            try:
                assert all(
                    [a >= 0 for a in [self.x_extent[0], self.y_extent[0], self.z_extent[0]]])
            except AssertionError:
                raise ValueError('Extents must be positive for the BOSS')

            # helper variable
            self.img_size = [self.x_extent[1] - self.x_extent[0],
                             self.y_extent[1] - self.y_extent[0],
                             self.z_extent[1] - self.z_extent[0]]
        else:
            self.img_size = None

    def get_img_info(self, z_slice):
        img = self.load_img(z_slice)

        width = img.shape[1]
        height = img.shape[0]
        datatype = img.dtype
        return (width, height, datatype)

    def validate_local_img(self, img_fname):
        if not os.path.isfile(img_fname):
            msg = '{} File not found: {}'.format(
                get_formatted_datetime(), img_fname)

            self.send_msg(msg, send_slack=True)
            self.num_READ_failures += 1
            if self.warn_missing_files:
                return None
            else:
                raise IOError(msg)
        return img_fname

    def load_s3_obj(self, img_fname, attempts=3):
        for attempt in range(attempts):
            try:
                obj = self.s3_res.Object(self.s3_bucket_name, img_fname)
                return io.BytesIO(obj.get()['Body'].read())
            except Exception as err:
                msg = '{} Exception {} occurred when getting image {} from s3'.format(
                    get_formatted_datetime(), err, img_fname)
                if attempt != attempts - 1:
                    time.sleep(2**(attempt + 1))

        self.send_msg(msg, send_slack=True)
        self.num_READ_failures += 1
        if self.warn_missing_files:
            return None
        else:
            raise IOError(msg)

    def load_render_slice(self, z_slice):
        self.send_msg('{} Getting slice {} from render.'.format(
            get_formatted_datetime(), z_slice))
        try:
            return self.render_obj.get_render_img(z_slice, window=self.render_window)
        except Exception as err:
            msg = '{} Exception {} occurred when getting image {} from render with error message {}'.format(
                get_formatted_datetime(), err, z_slice, str(err))
            self.num_READ_failures += 1
            if self.warn_missing_files:
                return None
            else:
                raise IOError(msg)

    def load_img(self, z_slice):
        if self.datasource == 'render':
            # download the slice from render server
            return self.load_render_slice(z_slice)

        # if it's not render datasource, we are working with images in some form
        img_fname = self.get_img_fname(z_slice)
        if self.datasource == 'local':
            # ensure the image exists on filesystem
            im_obj = self.validate_local_img(img_fname)
        elif self.datasource == 's3':
            # download the file from s3
            im_obj = self.load_s3_obj(img_fname)

        # called if datasource is s3 or local
        try:
            _, extension = os.path.splitext(img_fname)
            # if it's PNG we load it with PILLOW using the user specfied datatype
            if extension.lower() == '.png':
                im = np.array(Image.open(im_obj), dtype=self.datatype)

            # if it's not PNG, we try to load it using PILLOW if it's uint8 or uint16
            elif self.datatype == 'uint8' or self.datatype == 'uint16':
                im = np.array(Image.open(im_obj))

            # if it's not uint8 or uint16, we have to use tifffile (annotation)
            else:
                im = tifffile.imread(im_obj)

            return im

        except OSError:
            msg = '{} Problem opening file: {}'.format(
                get_formatted_datetime(), img_fname)
            self.send_msg(msg, send_slack=True)
            if self.warn_missing_files:
                return None
            raise OSError(msg)

        except Exception as err:
            msg = '{} Unknown error {}: {}'.format(
                get_formatted_datetime(), err, img_fname)
            self.send_msg(msg, send_slack=True)
            if self.warn_missing_files:
                return None
            raise IOError(msg)

    def get_img_fname(self, z_index):
        if self.datasource == 'render':
            return None

        base_path = self.base_path
        base_fname = self.base_fname

        if z_index >= self.z_range[1]:
            raise IndexError("Z-index out of range")

        matches = re.findall(r'<(p:\d+)?>', base_fname)
        for m in matches:
            if m:
                # There is zero padding
                z_str = str(z_index * self.z_step).zfill(int(m.split(':')[1]))
            else:
                z_str = str(z_index * self.z_step)
            base_fname = base_fname.replace("<{}>".format(m), z_str)

        # replace <ch> in filename with channel.name
        matches = re.findall('<(ch)>', base_fname)
        for m in matches:
            base_fname = base_fname.replace(
                "<{}>".format(m), self.ch_name)

        # replace <ch> in filename with channel.name
        matches = re.findall('<(ch)>', base_path)
        for m in matches:
            base_path = base_path.replace(
                "<{}>".format(m), self.ch_name)

        # prepend root, append extension
        return os.path.join(base_path, "{}.{}".format(base_fname, self.extension))

    def read_img_stack(self, z_slices):
        self.send_msg('{} Reading image data (z range: {}:{})'.format(
            get_formatted_datetime(), z_slices[0], z_slices[-1] + 1))

        start_time = time.time()
        im_array = np.zeros(
            (len(z_slices), self.img_size[1], self.img_size[0]), dtype=self.datatype, order='C')
        for idx, z_slice in enumerate(z_slices):
            img = self.load_img(z_slice)
            if img is None and self.warn_missing_files:
                continue
            im_array[idx, :, :] = img

        # cast the data as uint64 for the BOSS annotations even if the data is something else
        if self.datatype != 'uint64' and self.boss_datatype == 'uint64':
            im_array = im_array.astype('uint64')

        end_time = time.time()
        read_time = end_time - start_time
        self.send_msg('{} Finished reading image data (z range: {}:{}) in {:.2f} sec'.format(
            get_formatted_datetime(), z_slices[0], z_slices[-1] + 1, read_time))
        return im_array


def get_formatted_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def validate_limit(data_rng, limit):
    if data_rng is not None and limit is not None:
        if limit[0] < data_rng[0] or limit[1] > data_rng[1]:
            raise ValueError
