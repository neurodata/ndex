"""
Python 3 program to download & upload data from NeuroData
"""


version = "1.0.9"


def check_version():
    """
    Tells you if you have an old version of ndex.
    """
    import requests

    r = requests.get("https://pypi.python.org/pypi/ndexchange/json").json()
    r = r["info"]["version"]
    if r != version:
        print(
            "A newer version of ndex is available. "
            + "'pip install -U ndexchange' to update."
        )
    return r
