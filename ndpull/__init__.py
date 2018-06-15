"""
Python 3 program to download data from NeuroData
"""


version = "1.0.6"


def check_version():
    """
    Tells you if you have an old version of ndpull.
    """
    import requests
    r = requests.get('https://pypi.python.org/pypi/ndpull/json').json()
    r = r['info']['version']
    if r != version:
        print("A newer version of ndpull is available. " +
              "'pip install -U ndpull' to update.")
    return r
