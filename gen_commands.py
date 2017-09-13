#!/usr/bin/env python

import shlex

""" Script to generate ingest commands for multiple processes """

# Recommend copy this to a new location for editing


script = "ingest_large_vol.py"

source_type = 's3'  # either 'local' or 's3'
s3_bucket_name = "BUCKET_NAME"  # not needed for 'local' source_type
aws_profile = "default"  # not needed for 'local' source_type

boss_config_file = "neurodata.cfg"  # location on local system for boss API key

# Slack messages (optional but recommended)
slack_token = "slack_token"  # Slack token for sending Slack messages
slack_username = "SLACKUSER"  # your slack username

# boss metadata
collection = 'COLL'
experiment = 'EXP'
channel = 'Ch1'

# data_directory _with_ trailing slash
data_directory = "DATA_DIR/"
file_name = "FILENAME<p:4>"

# increment of filename numbering (always increment in steps of 1 in the boss, typically will be '1')
z_step = '1'

# extension name for images, supported image types are PNG and TIFF
file_format = 'tif'

# float or int
voxel_size = 'XXX YYY ZZZ'

# nanometers/micrometers/millimeters/centimeters
voxel_unit = 'micrometers'

# uint8 or uint16 for image channels, uint64 for annotations
data_type = 'uint16'

# pixel extent for images in x, y and number of total z slices
data_dimensions = "XXXX YYYY ZZZZ"

# first inclusive, last _exclusive_ list of sections to ingest, integers, typically the same as ZZZZ "data_dimensions"
zrange = [0, ZZZZ]

# Number of workers to use, watch for out of memory errors
workers = 6


range_per_worker = (zrange[1] - zrange[0]) // workers

print("# Range per worker: ", range_per_worker)

if range_per_worker % 16:  # supercuboids are 16 z slices
    range_per_worker = ((range_per_worker // 16) + 1) * 16

print("# Range per worker (rounded up): ", range_per_worker)

# amount of memory per worker
ddim_xy = list(map(int, data_dimensions.split(' ')[0:2]))
if data_type == 'uint8':
    mult = 1
elif data_type == 'uint16':
    mult = 2
elif data_type == 'uint64':
    mult = 8
mem_per_w = ddim_xy[0] * ddim_xy[1] * mult * 16 / 1024 / 1024 / 1024
print('Expected memory usage per worker {:.1f} GB'.format(mem_per_w))

# amount of memory total
mem_tot = mem_per_w * workers
print('Expected total memory usage: {:.1f} GB'.format(mem_tot))


cmd = "python " + script + " "
cmd += ' --base_path ' + shlex.quote(data_directory)
cmd += ' --base_filename ' + shlex.quote(file_name)
cmd += ' --extension ' + file_format
cmd += ' --datasource ' + source_type
cmd += ' --collection ' + collection
cmd += ' --experiment ' + experiment
cmd += ' --channel ' + channel
cmd += ' --voxel_size ' + voxel_size
cmd += ' --voxel_unit ' + voxel_unit
cmd += ' --datatype ' + data_type
cmd += ' --img_size ' + data_dimensions
cmd += ' --z_range ' + ' '.join(list(map(str, zrange)))
cmd += ' --boss_config_file ' + boss_config_file
cmd += ' --slack_token_file ' + slack_token
if source_type == 's3':
    cmd += " --s3_bucket_name " + s3_bucket_name
    cmd += ' --aws_profile ' + aws_profile
cmd += ' --create_resources '
print(cmd)

for worker in range(workers):
    start_z = worker * range_per_worker + zrange[0]
    if start_z < zrange[0]:
        start_z = zrange[0]
    if start_z > zrange[1]:
        # No point start a useless thread
        continue
    end_z = min(zrange[1], (worker + 1) * range_per_worker + zrange[0])

    cmd = "python " + script + " "
    cmd += ' --base_path ' + shlex.quote(data_directory)
    cmd += ' --base_filename ' + shlex.quote(file_name)
    cmd += ' --extension ' + file_format
    cmd += ' --datasource ' + source_type
    cmd += ' --collection ' + collection
    cmd += ' --experiment ' + experiment
    cmd += ' --channel ' + channel
    cmd += ' --voxel_size ' + voxel_size
    cmd += ' --voxel_unit ' + voxel_unit
    cmd += ' --datatype ' + data_type
    cmd += ' --img_size ' + data_dimensions
    cmd += ' --z_range %d %d ' % (start_z, end_z)
    cmd += ' --warn_missing_files'
    cmd += ' --boss_config_file ' + boss_config_file
    cmd += ' --slack_token_file ' + slack_token
    cmd += ' --z_step ' + z_step

    cmd += " --slack_usr " + slack_username
    if source_type == 's3':
        cmd += " --s3_bucket_name " + s3_bucket_name
        cmd += ' --aws_profile ' + aws_profile
    cmd += " &"

    print(cmd)
