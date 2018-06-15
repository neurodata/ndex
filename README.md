# ndpull

[![PyPI version](https://badge.fury.io/py/ndpull.svg)](https://badge.fury.io/py/ndpull) [![Build Status](https://travis-ci.org/neurodata/ndpull.svg?branch=master)](https://travis-ci.org/neurodata/ndpull) [![Coverage Status](https://coveralls.io/repos/github/neurodata/ndpull/badge.svg?branch=master)](https://coveralls.io/github/neurodata/ndpull?branch=master)

Python 3 command line program to download data from NeuroData.  Can download a full stack of data or with specific limits.  View available data at [ndwebtools](https://ndwebtools.neurodata.io/) or [neurodata.io](https://neurodata.io/)

## Install

- Create a python 3 [virtual environment](https://virtualenv.pypa.io/en/stable/)

  `virtualenv env`
- Activate virtual environment
- Install
  - Via pypi (Preferred)

    `pip install ndpull`
  - From github (Latest dev version)

    `pip install git+git://github.com/neurodata/ndpull.git`

## Config

Generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (example provided: [neurodata.cfg.example](neurodata.cfg.example))

## Run

### Command line

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

### Python

See [example.py](example.py)
