from ndpull import ndpull

collection = 'kharris15'
experiment = 'apical'
channel = 'em'

# see neurodata.cfg.example to generate your own
config_file = 'neurodata.cfg'

# print metadata
meta = ndpull.BossMeta(collection, experiment, channel)
token, boss_url = ndpull.get_boss_config(config_file)
rmt = ndpull.BossRemote(boss_url, token, meta)
print(rmt)  # prints metadata

# download slices with these limits:
x = [4096, 4608]
y = [4608, 5120]
z = [90, 100]

# returns a namespace as a way of passing arguments
result = ndpull.collect_input_args(
    collection, experiment, channel, config_file, x=x, y=y, z=z, res=0, outdir='./')

# downloads the data
ndpull.download_slices(result, rmt)
