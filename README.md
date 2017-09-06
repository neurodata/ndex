# ingest_large_vol

Python 3 command line program to ingest large volume image data into the cloud using APL's BOSS spatial database: (<https://github.com/jhuapl-boss>)

Supports loading images from local storage or an AWS S3 bucket.

This program loads 16 separate image files (either PNG or TIFF) at a time into memory and POSTs the data in smaller blocks to avoid write locks by the Boss.  With large image sizes this can be memory intensive.  This tool can be run simultaneously with non overlapping z-slices to increase the speed of the ingest (assisting program `gen_commands.py`).

Running the script will log its behavior to a file (`log.txt`) and can optionally send Slack messages when it finishes a job and if it encounters errors.

## Install

1. Clone this repository
1. Create virtualenv (linux: `virtualenv env -p /usr/bin/python3`)
1. Activate virtualenv (linux: `source env/bin/activate`)
1. Install dependencies: `pip install -r requirements.txt`
1. generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (example: `neurodata.cfg.example`)
1. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to file `slack_token`

## Run

* To generate list of commands, edit the `gen_commands.py` example file, adding your experiment details, and run it (`python3 gen_commands.py`).  It will generate the complete command line(s) needed to do the ingest job and estimate the amount of memory needed.  You can then run those commands.
* Alternatively, run: `python3 ingest_large_vol.py -h` to see the complete list of command line options.
