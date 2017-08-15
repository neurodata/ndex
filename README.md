# ingest_large_vol
Python 3 commmand line program to ingest large volume image data into the cloud using APL's BOSS spatial database: (https://github.com/jhuapl-boss)

Supports loading images from local storage or an S3 bucket.

This program loads 16 image files (either PNG or TIFF) at a time into memory and POSTs the data in smaller blocks to avoid write locks by the Boss.  With large image sizes it can be memory intensive.  Can be run simultaneously with non overlapping z-slices to increase the speed of the ingest.

Running the script will log its behavior to a file (`log.txt`) and can optionally send Slack messages when it finishes a job and if it encouters errors.

### Install:
1. clone this directory
2. create virtualenv
3. pip install -r requirements.txt
4. generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to `neurodata.cfg` (example: `neurodata.cfg.example`)
5. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to `slack_token`

### Run:
* `python ingest_large_vol.py -h`
