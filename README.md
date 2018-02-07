# ndpush

Python 3 command line program to ingest large volume image and annotation data into the cloud using APL's BOSS spatial database: (<https://github.com/jhuapl-boss>)

Supports loading images from local storage, an AWS S3 bucket, or [render](https://github.com/saalfeldlab/render).

This program loads 16 separate image files (either PNG or TIFF) at a time into memory and POSTs the data in blocks for optimal performance with the block level storage of the BOSS.  With large image sizes this can be memory intensive.  This tool can be run simultaneously with non overlapping z-slices to increase the speed of the ingest (assisting program `gen_commands.py`).

Running the script will log its behavior to a file and can optionally send Slack messages when it finishes a job or if it encounters errors.

## Install

1. Install or insure you have Python 3 (x64)
    - `python --version`
1. Clone this repository
    1. `git clone https://github.com/neurodata/ndpush.git`
1. Create virtual environment for ingest jobs
    1. Linux
        ```bash
        > nd ndpush
        > python3 -m venv ENV
        > source ENV/bin/activate
        ```
    1. Windows
        ```dos
        > cd ndpush
        > python -m venv ENV
        > .\ENV\Scripts\activate
        ```
1. Install compiler for Windows
    1. [Visual C++ 2015 Build Tools](http://landinghub.visualstudio.com/visual-cpp-build-tools)
1. Install requirements
    ```bash
    > pip install -r requirements.txt
    ```
1. generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (example provided: `neurodata.cfg.example`).
1. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to file `slack_token`.
1. To perform ingest, you must have `resource manager` permissions in the BOSS (talk to an admin to get these privileges).

## Run

* To generate an ingest's command line arguments, edit a new file copied from `gen_commands.example.py` example file.
  * Add your experiment details, and run it (`python gen_commands.py`).  It will generate command lines to run and estimate the amount of memory needed.  You can then copy and run those commands.
* Alternatively, run: `python ingest_large_vol.py -h` to see the complete list of command line options.