# ingest_large_vol

Python 3 command line program to ingest large volume image data into the cloud using APL's BOSS spatial database: (<https://github.com/jhuapl-boss>)

Supports loading images from local storage or an AWS S3 bucket.

This program loads 16 separate image files (either PNG or TIFF) at a time into memory and POSTs the data in smaller portions (to avoid write locks by the Boss).  With large image sizes this can be memory intensive.  This tool can be run simultaneously with non overlapping z-slices to increase the speed of the ingest (assisting program `gen_commands.py`).

Running the script will log its behavior to a file (`log.txt`) and can optionally send Slack messages when it finishes a job and if it encounters errors.

## Install

1. Install or insure you have Python 3 (x64)
1. Clone this repository
    1. `git clone https://github.com/neurodata/ingest_large_vol.git`
1. Install virtualenv
    1. Linux
        ```bash
        > pip3 install virtualenv
        ```
    1. Windows
        ```dos
        > cd C:\Users\USERNAME\AppData\Local\Programs\Python\Python36\Scripts\
        > pip.exe install virtualenv
        ```
1. Create virtual environment for ingest jobs
    1. Linux
        ```bash
        > virtualenv ENV -p /usr/bin/python3
        > source ENV/bin/activate
        ```
    1. Windows
        ```dos
        > virtualenv.exe C:\Users\USERNAME\ENV -p C:\Users\USERNAME\AppData\Local\Programs\Python\Python36\python.exe
        > cd INGEST_LARGE_VOL_PATH
        > C:\Users\USERNAME\ENV\Scripts\activate.bat
        ```
1. Install compiler for Windows
    1. [Visual C++ 2015 Build Tools](http://landinghub.visualstudio.com/visual-cpp-build-tools)
1. Install requirements
    ```bash
    > pip install -r requirements.txt
    ```
1. generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (example provided: `neurodata.cfg.example`)
1. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to file `slack_token`
1. To perform ingest, you must have `resource manager` permissions (talk to an admin to get these privileges)

## Run

* To generate list of commands, edit the `gen_commands.py` example file
  * Add your experiment details, and run it (`python3 gen_commands.py`).  It will generate the complete command line(s) needed to do the ingest job and estimate the amount of memory needed.  You can then copy and run those commands.
* Alternatively, run: `python3 ingest_large_vol.py -h` to see the complete list of command line options.
