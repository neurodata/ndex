# ndex

[![PyPI version](https://badge.fury.io/py/ndexchange.svg)](https://badge.fury.io/py/ndexchange) 
[![Build Status](https://travis-ci.org/neurodata/ndex.svg?branch=master)](https://travis-ci.org/neurodata/ndex)
[![Coverage Status](https://coveralls.io/repos/github/neurodata/ndpull/badge.svg?branch=master)](https://coveralls.io/github/neurodata/ndpull?branch=master)

Python 3 command-line program to exchange (download/upload) image data with NeuroData's cloud deployment of APL's BOSS spatial database: <https://github.com/jhuapl-boss>.  View available data at [ndweb](https://ndwebtools.neurodata.io/) or [neurodata.io](https://neurodata.io/).

## Features

- Download data as TIFF-stacks or as individual slices
- Download the full extent of data or specify spatial limits/coordinates of a resource
- Can be used in Python environment to store images to disk
- Upload (ingest) large volume image and annotation data (TIFF/OME/PNG supported)
- Uploads from local storage, [AWS S3](https://aws.amazon.com/s3/) bucket, or [render](https://github.com/saalfeldlab/render)
- Uploads log to a file and can optionally send Slack messages when it finishes or if it encounters errors

## Considerations

Uploading loads 16 images/slices (either PNG or TIFF) at a time into memory and POSTs the data in blocks for optimal network performance with the block level storage of the BOSS.  With large image tiles this can be memory intensive (16GB or more ram recommended).  This tool can be run simultaneously with non overlapping z-slices to increase the speed of the ingest (assisting program `gen_commands.py`).

**Note:** Formerly two separate programs: [ndpull](https://github.com/neurodata-arxiv/ndpull) & [ndpush](https://github.com/neurodata-arxiv/ndpush).

## Install

- Install or insure you have [Python 3](https://www.python.org/downloads/) (x64).  Versions 3.5, 3.6, 3.7 supported

  `python --version`
- Create a python 3 [virtual environment](https://virtualenv.pypa.io/en/stable/)

  `virtualenv env`
- Activate virtual environment

- Install compiler for Windows

  - [Visual C++ Build Tools](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2017)

- Install
  - Via PyPI (Preferred)

    `pip install ndexchange`
  - From github (Latest dev version)

    `pip install git+git://github.com/neurodata/ndex.git`

## Config

1. Register for a free account and generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (using the format provided in [neurodata.cfg.example](examples/neurodata.cfg.example))

1. To send messages through Slack (optional) you will also need a [Slack API key](https://api.slack.com/custom-integrations/legacy-tokens) and save to file `slack_token`.

1. To perform ingest, you must have `resource manager` permissions in the BOSS (ask an [admin](mailto:support@neurodata.io) to get these privileges).

## Download images (ndpull)

### Command line usage

*Note: please use PowerShell if on Windows*

To see the full list of options, run the following command:

`ndpull --help`

Example command:

```sh
> ndpull --config_file neurodata.cfg --collection kharris15 --experiment apical --channel em --x 4096 4608 --y 4608 5120 --z 90 100 --outdir .
```

### Python usage (from within Jupyter notebook, script, or IDE)

See [example.py](examples/example_ndpull.py)

## Upload images (ndpush)

- Please contact NeuroData for required (resource-manager) privileges before starting an ingest.
- To generate an ingest's command line arguments, create and edit a file copied from provided example: [gen_commands.example.py](examples/gen_commands.example.py).
- Add your experiment details and run it from within the activated python environment (`python gen_commands.py`).  It will generate command lines to run and estimate the amount of memory needed.  You can then copy and run those commands.
- Alternatively, run: `ndpush -h` to see the complete list of command line options.

### Expand stacks

Currently, tiff stacks are not natively supported for ingest.  We provide a [script](scripts/expand_stacks.py) to expand a tiff stack to disk which can be run prior to doing an ingest.  [Fiji](https://fiji.sc/) could be used instead (`Save as... Image Sequence...`).  After ndex is installed, `expand_stacks` should be available to use from the command line:

```sh
usage: expand_stacks.py [-h] [--datatype DATATYPE] [--split_RGB]
                        tiffstack [outpath]
```

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

## Publish new version

Increment the version number in `ndex/__init__.py`

create a ~/.pypirc file with the following content:

```ini
[distutils]
index-servers =
  pypi

[pypi]
username=xxxx
password=xxxx
```

Upload to PyPi

```bash
pip install --upgrade setuptools wheel
pip install --upgrade twine
python setup.py sdist bdist_wheel
twine upload dist/*
```

Pin version on GitHub releases