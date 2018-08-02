import os
import time

import pytest

from ....parse_log import *


class TestParseLog:

    def test_parse_log(self):
        log_data = '''Resources set up. Collection: ben_dev, Experiment: dev_ingest_2, Channel: def_files
2017-09-14 11:16:05 Reading image data (z range: 1:16)
2017-09-14 11:27:50 Finished reading image data
2017-09-14 11:27:54 POST succeeded in 2.28 sec. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (0, 512), z: (0, 16)
2017-09-14 11:27:55 POST succeeded in 1.27 sec. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (512, 1024), z: (0, 16)
Create cutout failed on Ch0, got HTTP response: (400) - {"message": "Error during write_cuboid: ('Resource Locked', 'The requested resource is locked due to excessive write errors. Contact support.', <ErrorCodes.RESOURCE_LOCKED: 108>)", "status": 400, "code": 9001}
2017-09-20 06:17:16 Error: data upload failed after multiple attempts, skipping. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (1024, 1536), z: (0, 16)
Create cutout failed on Ch0, got HTTP response: (400) - {"message": "Error during write_cuboid: ('Resource Locked', 'The requested resource is locked due to excessive write errors. Contact support.', <ErrorCodes.RESOURCE_LOCKED: 108>)", "status": 400, "code": 9001}
2017-09-20 06:17:16 Error: data upload failed after multiple attempts, skipping. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (1536, 2048), z: (0, 16)
2017-09-14 11:27:55 POST succeeded in 1.27 sec. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (1024, 1536), z: (0, 16)
2017-09-14 11:27:55 POST succeeded in 1.27 sec. Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (512, 1024), y: (0, 512), z: (0, 16)
'''

        logfile = 'log_test.txt'
        with open(logfile, 'w') as f:
            f.write(log_data)

        repeatfile = 'repeat_cutouts_test.txt'

        # parse the log file to generate the repeat_cutouts file
        repeatfile = parse_log(logfile, repeatfile)
        with open(repeatfile, 'r') as f:
            repeatdata = f.readlines()

        repeatdata_valid = 'Coll: ben_dev, Exp: dev_ingest_2, Ch: def_files, x: (0, 512), y: (1536, 2048), z: (0, 16)\n'
        assert repeatdata == [repeatdata_valid]

        # cleanup
        os.remove(repeatfile)
        os.remove(logfile)
