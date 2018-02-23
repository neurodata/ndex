#!/usr/bin/env python

import os
import shlex
from subprocess import list2cmdline

""" Script to generate ingest commands for ingest program """
""" Once generated, copy commands to terminal and run them """

""" Recommend copy this to a new location for editing """


script = "ingest_large_vol.py"

source_type = 's3'  # either 'local', 's3', or 'render'

# only used for 's3' source_type
s3_bucket_name = "BUCKET_NAME"
aws_profile = "default"

# only used for 'render' source_type
render_owner = 'OWNER_NAME'
render_project = 'PROJECT_NAME'
render_stack = 'STACK_NAME'
render_channel = 'CHANNEL_NAME'  # can be None if no channels in the stack
render_baseURL = 'BASEURL'
# 1 is full resolution, .5 is downsampled in half. None is scale = 1. Powers of 2 (e.g. .5, .25, .125)
render_scale = 1
render_window = '0 10000'  # set to None no windowing will be applied for 16bit to 8bit


boss_config_file = "neurodata.cfg"  # location on local system for boss API key

# Slack messages (optional)
# sends a slack message when ingest is finished with a link to the see the data
# set to a blank string (e.g. '') to exclude from command output
slack_token = "slack_token"  # Slack token for sending Slack messages
slack_username = "SLACKUSER"  # your slack username

# boss metadata
collection = 'COLL'
experiment = 'EXP'

# a single  channel name or None if there are multiple channels
channel = 'Ch1'
# channel = None

# path to a text file with names for each channel
# channels_list_file = 'channels.txt'
channels_list_file = None

# data_directory _with_ trailing slash
# <ch> (if needed) indicates where the program will insert the channel name for paths when iterating over multiple channels
# can be ignored for 'render' data source
data_directory = "DATA_DIR/<ch>/"
# data_directory = None

# filename without extension (no '.tif')
# <p:4> indicates the z index of the tif file, with up to N leading zeros (4 in this example)
# <ch> indicates where the program will insert the channel name for file names when iterating over multiple channels (optional)
# can be ignored for 'render' data source
file_name = "FILENAME<ch>-<p:4>"
# file_name = None

# extension name for images, supported image types are PNG and TIFF
# set to None for render source_type
# extension needs to match the full filenames and can be any string (e.g.: ome, tif, png)
file_format = 'tif'
# file_format = None

# increment of filename numbering (always increment in steps of 1 in the boss, typically will be '1')
# set to None for render source_type
z_step = '1'
# z_step = None

# float or int supported
voxel_size = 'XXX YYY ZZZ'

# nanometers/micrometers/millimeters/centimeters
voxel_unit = 'micrometers'

# uint8 or uint16 for image channels, uint64 for annotations
data_type = 'uint16'

# name of the reference channel (in the same experiment) for an annotation channel(s)
# set to None for image data
# Warning: if set to any value other than None, uploaded data will be treated as 'annotation' type
reference_channel = None

# pixel -/+ extent (integers) for images in x (width), y (height) and z (slices)
# not used for render, comment out
x_extent = [0, X]
y_extent = [0, Y]

# optional for render (used w/ mult. workers)
z_extent = [0, Z]

# if any of the extents are negative, they need to be offset to >= 0 for the boss
offset_extents = False

# you can manually force offsets for a channel.
# Useful when there are a handful of mult. channels, with different negative extents,
# that you want to have in the same shared volume
# note that if using render scale, this should be in the scaled coordinates
forced_offsets = None
# forced_offsets = [0, 0, 0]

# only need to specify if forcing a particular coordinate frame extent
# note that if using render scale, this should be in the scaled coordinates
coord_frame_x_extent = None
coord_frame_y_extent = None
coord_frame_z_extent = None
# coord_frame_x_extent = [0, X]
# coord_frame_y_extent = [0, Y]
# coord_frame_z_extent = [0, Z]


# first inclusive, last _exclusive_ list of sections to ingest for _this_ job (can be negative)
# typically the same as Z "extent"
# optional for render (used w/ mult. workers), requires z_extent (above)
zrange = [0, Z]
# if it's a render data source, we (optionally) get the entire z range from the metadata
# (forces single worker)
# zrange = None

# to crop the source data to a region of interest use these values.  Otherwise, set to None
# currently only works in render
limit_x = None
limit_y = None
limit_z = None
# limit_x = [XLIMLOW, XLIMHIGH]
# limit_y = [YLIMLOW, YLIMHIGH]
# limit_z = [ZLIMLOW, ZLIMHIGH]


# Number of workers to use
# each worker loads additional 16 image files so watch out for out of memory errors
# ignored if zrange is None
workers = 1


""" Code to generate the commands """


def gen_comm(zstart, zend):
    cmd = "python {}".format(script)
    cmd += ' --datasource {}'.format(source_type)
    if source_type != 'render':
        if os.name == 'nt':
            cmd += ' --base_path {}'.format(list2cmdline([data_directory]))
            cmd += ' --base_filename {}'.format(list2cmdline([file_name]))
        else:
            cmd += ' --base_path {}'.format(shlex.quote(data_directory))
            cmd += ' --base_filename {}'.format(shlex.quote(file_name))
        cmd += ' --extension {}'.format(file_format)
        cmd += ' --x_extent {d[0]} {d[1]}'.format(d=x_extent)
        cmd += ' --y_extent {d[0]} {d[1]}'.format(d=y_extent)
        cmd += ' --z_step {}'.format(z_step)
        cmd += ' --warn_missing_files'

    if limit_x is not None:
        cmd += ' --limit_x {d[0]} {d[1]}'.format(d=limit_x)
    if limit_y is not None:
        cmd += ' --limit_y {d[0]} {d[1]}'.format(d=limit_y)
    if limit_z is not None:
        cmd += ' --limit_z {d[0]} {d[1]}'.format(d=limit_z)

    if 'z_extent' in globals() and 'zstart' in locals() and 'zend' in locals():
        cmd += ' --z_extent {d[0]} {d[1]}'.format(d=z_extent)
        cmd += ' --z_range %d %d ' % (zstart, zend)
    else:
        # getting this directly from render
        if source_type == 'render':
            pass
        else:
            raise NameError

    if offset_extents:
        cmd += ' --offset_extents'

    if forced_offsets:
        cmd += ' --forced_offsets {d[0]} {d[1]} {d[2]}'.format(
            d=forced_offsets)

    if coord_frame_x_extent:
        cmd += ' --coord_frame_x_extent {d[0]} {d[1]}'.format(
            d=coord_frame_x_extent)
    if coord_frame_y_extent:
        cmd += ' --coord_frame_y_extent {d[0]} {d[1]}'.format(
            d=coord_frame_y_extent)
    if coord_frame_z_extent:
        cmd += ' --coord_frame_z_extent {d[0]} {d[1]}'.format(
            d=coord_frame_z_extent)

    if source_type == 's3':
        cmd += " --s3_bucket_name {}".format(s3_bucket_name)
        cmd += ' --aws_profile {}'.format(aws_profile)

    if source_type == 'render':
        cmd += ' --render_owner {}'.format(render_owner)
        cmd += ' --render_project {}'.format(render_project)
        cmd += ' --render_stack {}'.format(render_stack)
        cmd += ' --render_baseURL {}'.format(render_baseURL)
        cmd += ' --render_scale {}'.format(render_scale)
        if render_window is not None:
            cmd += ' --render_window {}'.format(render_window)
        if render_channel is not None:
            cmd += ' --render_channel {}'.format(render_channel)

    cmd += ' --collection {}'.format(collection)
    cmd += ' --experiment {}'.format(experiment)
    if channel is not None:
        cmd += ' --channel {}'.format(channel)
    else:
        cmd += ' --channels_list_file {}'.format(channels_list_file)
    cmd += ' --voxel_size {}'.format(voxel_size)
    cmd += ' --voxel_unit {}'.format(voxel_unit)
    cmd += ' --datatype {}'.format(data_type)
    if reference_channel is not None:
        cmd += ' --source_channel {}'.format(reference_channel)
    cmd += ' --boss_config_file {}'.format(boss_config_file)

    if slack_token != '' and slack_username != '':
        cmd += ' --slack_token_file {}'.format(slack_token)
        cmd += " --slack_usr {}".format(slack_username)

    return cmd


if zrange:
    # generate command with zrange
    range_per_worker = (zrange[1] - zrange[0]) // workers

    print("# Range per worker: ", range_per_worker)

    if range_per_worker % 16:  # supercuboids are 16 z slices
        range_per_worker = ((range_per_worker // 16) + 1) * 16

    print("# Range per worker (rounded up): ", range_per_worker)

    try:
        if x_extent:
            # amount of memory per worker
            ddim_xy = [x_extent[1] - x_extent[0], y_extent[1] - y_extent[0]]
            if data_type == 'uint8':
                mult = 1
            elif data_type == 'uint16':
                mult = 2
            elif data_type == 'uint64':
                mult = 8
            mem_per_w = ddim_xy[0] * ddim_xy[1] * \
                mult * 16 / 1024 / 1024 / 1024
            print(
                '# Expected memory usage per worker {:.1f} GB'.format(mem_per_w))

            # amount of memory total
            mem_tot = mem_per_w * workers
            print('# Expected total memory usage: {:.1f} GB'.format(mem_tot))
    except NameError:
        if source_type == 'render':
            pass
        else:
            raise NameError

    cmd = gen_comm(zrange[0], zrange[1])
    cmd += ' --create_resources'
    print('\n' + cmd + '\n')

    for worker in range(workers):
        start_z = max((worker * range_per_worker +
                       zrange[0]) // 16 * 16, zrange[0])
        if start_z < zrange[0]:
            start_z = zrange[0]
        if start_z > zrange[1]:
            # No point start a useless thread
            continue

        next_z = ((worker + 1) * range_per_worker + zrange[0]) // 16 * 16
        end_z = min(zrange[1], next_z)

        cmd = gen_comm(start_z, end_z)
        cmd += " &"
        print(cmd + '\n')

else:
    # generate a single command without zrange
    cmd = gen_comm(None, None)
    cmd += ' --create_resources'
    print(cmd + '\n')

    cmd = gen_comm(None, None)
    cmd += ' &'
    print(cmd + '\n')
