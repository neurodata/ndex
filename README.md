[![Build Status](https://travis-ci.org/neurodata/ndex.svg?branch=master)](https://travis-ci.org/neurodata/ndex)  [![Coverage Status](https://coveralls.io/repos/github/neurodata/ndpull/badge.svg?branch=master)](https://coveralls.io/github/neurodata/ndpull?branch=master)

# ndex

Python 3 command line programs to exchange (download/upload) image data with NeuroData's cloud deployment of APL's BOSS spatial database: <https://github.com/jhuapl-boss>.  View available data at [ndweb](https://ndwebtools.neurodata.io/) or [neurodata.io](https://neurodata.io/).  

Features:

- Can download a full TIFF-stack of data or specify spatial limits/coordinates for download
- Upload (ingest) large volume image and annotation data
- Supports upload from local storage, AWS S3 bucket, or [render](https://github.com/saalfeldlab/render)
- Uploads log to a file and can optionally send Slack messages when it finishes a job or if it encounters errors

Uploading loads 16 separate image files (either PNG or TIFF) at a time into memory and POSTs the data in blocks for optimal network performance with the block level storage of the BOSS.  With large image tiles this can be memory intensive (16GB ram recommended).  This tool can be run simultaneously with non overlapping z-slices to increase the speed of the ingest (assisting program `gen_commands.py`).

**Note:** Formerly two separate programs: [ndpull](https://github.com/neurodata/ndpull) and [ndpush](https://github.com/neurodata/ndpush).

## Install

- Install or insure you have Python 3 (x64)
  
  `python --version`
- Create a python 3 [virtual environment](https://virtualenv.pypa.io/en/stable/)

  `virtualenv env`
- Activate virtual environment

- Install compiler for Windows

  - [Visual C++ 2015 Build Tools](http://landinghub.visualstudio.com/visual-cpp-build-tools)

- Install
  - Via PyPI (Preferred)

    `pip install ndex`
  - From github (Latest dev version)

    `pip install git+git://github.com/neurodata/ndex.git`

## Config

1. Register for a free account and generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (using the format provided in [neurodata.cfg.example](examples/neurodata.cfg.example))

1. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to file `slack_token`.

1. To perform ingest, you must have `resource manager` permissions in the BOSS (ask an [admin](mailto:support@neurodata.io) to get these privileges).

## Download images (ndpull)

### Command line usage

```sh
> ndpull --help
usage: ndpull [-h] [--config_file CONFIG_FILE] [--token TOKEN] [--url URL]
              [--collection COLLECTION] [--experiment EXPERIMENT]
              [--channel CHANNEL] [--x X X] [--y Y Y] [--z Z Z] [--res RES]
              [--outdir OUTDIR] [--full_extent] [--print_metadata]
              [--threads THREADS] [--iso]

optional arguments:
  -h, --help            show this help message and exit
  --config_file CONFIG_FILE
                        User config file for BOSS
  --token TOKEN         User token for the boss (not used if config file
                        specified)
  --url URL             URL to boss endpoint (not used if config file
                        specified)
  --collection COLLECTION
                        Collection
  --experiment EXPERIMENT
                        Experiment
  --channel CHANNEL     Channel
  --x X X               X range for stack
  --y Y Y               Y range for stack
  --z Z Z               Z range for stack
  --res RES             Stack resolution
  --outdir OUTDIR       Path to output directory.
  --full_extent         Use the full extent of the data on the BOSS
  --print_metadata      Prints the metadata on the
                        collection/experiment/channel and quits
  --threads THREADS     Number of threads for downloading data.
  --iso                 Returns iso data (for downsampling in z)
```

To run

```sh
> ndpull --config_file neurodata.cfg --collection kharris15 --experiment apical --channel em --x 4096 4608 --y 4608 5120 --z 90 100 --outdir .
```

### Python usage

See [example.py](examples/example.py)

## Upload images (ndpush)

- Please contact NeuroData for required privileges before starting an ingest.

- To generate an ingest's command line arguments, create and edit a file copied from provided example: [gen_commands.example.py](examples/gen_commands.example.py).

- Add your experiment details and run it from within the activated python environment (`python gen_commands.py`).  It will generate command lines to run and estimate the amount of memory needed.  You can then copy and run those commands.

- Alternatively, run: `ndpush -h` to see the complete list of command line options.

## Testing

We use [pytest](https://pytest.org/) as our testing library.  To run the tests:

- Install testing requirements in your virtual environment: `pip install pytest`

For running with vscode (where you can set the env file) or on travis-ci:
- Create a `.env` file and copy the following line of code, inserting your own API token into it:
  ```ini
  BOSS_TOKEN=<<your token from https://api.boss.neurodata.io/token/ here>>
  SLACK_TOKEN=<your token from https://api.slack.com/custom-integrations/legacy-tokens here>>
  ```
- Configure your test environment to load that file into your environmental variables (in [vscode](https://code.visualstudio.com/docs/python/environments#_where-the-extension-looks-for-environments) set the `python.envFile` option to `"${workspaceFolder}/.env"`)
- Follow this guide to add the tokens to your travis.yml file:
https://docs.travis-ci.com/user/encryption-keys/

If running pytest from the command line, create a pytest.ini file in the root directory ([example](examples/pytest.ini.example))
- Install plugin for pytest to automatically load the environment variables: `pip isntall pytest-env`
- Run all the tests from the command line: `python -m pytest`
- Or for a particular function in a file: `python -m pytest .\tests\test_ndpull.py::Testndpull::test_print_meta`

Notes:
- You'll need to edit the tests to use your slack username
- Some tests may fail as a result of not having access to specific BOSS resources.  Either modify the tests to use different resources or contact NeuroData to gain access (specifically need to be added to the `dev` group in the BOSS).
