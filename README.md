# ndpull
Python 3 command line tool to get data from NeuroData.  Can download a full stack of data or specify limits.  View available data at [ndwebtools](https://ndwebtools.neurodata.io/) or [neurodata.io](https://neurodata.io/)

## Install
1. `git clone https://github.com/neurodata/ndpull.git`
1. `virtualenv env -p python3`
    1. LINUX: `env/bin/activate`
    1. WIN: `.\env\Scripts\activate` (Change permissions if on Powershell: [guide](https://virtualenv.pypa.io/en/stable/userguide/#activate-script))
1. `pip install -r requirements.txt`

## Config
1. generate a [Boss API key](https://api.boss.neurodata.io/v1/mgmt/token) and save it to file named `neurodata.cfg` (example provided: [neurodata.cfg.example](neurodata.cfg.example))

## Run

```dos
python .\stackDownload.py --help
```

```dos
usage: stackDownload.py [-h] [--config_file CONFIG_FILE] [--token TOKEN]
                        [--url URL] [--collection COLLECTION]
                        [--experiment EXPERIMENT] [--channel CHANNEL]
                        [--x X X] [--y Y Y] [--z Z Z] [--res RES]
                        [--outdir OUTDIR] [--full_extent] [--print_metadata]

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
```

