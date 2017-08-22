#!/usr/bin/env python

import shlex

""" Script to generate ingest commands for multiple processes """


script = "ingest_large_vol.py"
s3_bucket_name = "BUCKET_NAME"
boss_config_file = "neurodata.cfg"
slack_token = "slack_token"
slack_username = "SLACKUSER"

# data_directory _with_ trailing slash
data_directory = "DATA_DIR/"
experiment = 'EXP'
file_name = "FILENAME<p:4>"
z_step = '1'
channel = 'Ch1'
file_format = 'tif'
source_type = 's3'
collection = 'COLL'
voxel_size = 'XXX YYY ZZZ'  # float/int
voxel_unit = 'micrometers'
data_type = 'uint16'
data_dimensions = "XXXX YYYY ZZZZ"
# first inclusive, last _exclusive_ list of sections to ingest, integers
zrange = [0, ZZZZ]
workers = 6    # Number of workers to use, watch for out of memory errors


range_per_worker = (zrange[1] - zrange[0]) // workers

print("# Range per worker: ", range_per_worker)

if range_per_worker % 16:  # supercuboids are 16 z slices
    range_per_worker = ((range_per_worker // 16) + 1) * 16

print("# Range per worker (rounded up): ", range_per_worker)

for worker in range(workers):
    start_z = worker * range_per_worker
    if start_z < zrange[0]:
        start_z = zrange[0]
    if start_z > zrange[1]:
        # No point start a useless thread
        continue
    end_z = min(zrange[1], (worker + 1) * range_per_worker)

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
    cmd += " --s3_bucket_name " + s3_bucket_name
    cmd += " &"

    print(cmd)
    if worker == 0:
        print("sleep 15")
