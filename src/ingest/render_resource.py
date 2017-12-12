import io
import random
import time
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool

import numpy as np
import requests
from PIL import Image

# render web service view
# http://render-dev-eric.neurodata.io/render-ws/view/index.html?

# render stack view
# http://render-dev-eric.neurodata.io/render-ws/view/stacks.html?renderStackOwner=Forrest&renderStackProject=H16_03_005_HSV_HEF1AG65_R2An15dTom&renderStack=ACQTdtomato

# swagger Web Service API
# http://render-dev-eric.neurodata.io/swagger-ui/

# metadata
# https://render-dev-eric.neurodata.io/render-ws/v1/owner/6_ribbon_experiments/project/M321160_Ai139_smallvol/stack/Acquisition_1_PSD95

# image
# https://render-dev-eric.neurodata.io/render-ws/v1/owner/6_ribbon_experiments/project/M321160_Ai139_smallvol/stack/Acquisition_1_PSD95/z/17/box/0,0,4096,4096,0.25/png-image?minIntesnity=0&maxIntensity=5000


class renderResource:
    def __init__(self, owner, project, stack, baseURL, scale=None):
        self.owner = owner
        self.project = project
        self.stack = stack
        self.baseURL = baseURL
        if scale is None:
            self.scale = 1
        else:
            self.scale = scale

        # self.level = math.log(1 / scale, 2)
        self.session = requests.Session()

        self.set_metadata()

    def set_metadata(self):
        metaURL = '{}owner/{}/project/{}/stack/{}'.format(
            self.baseURL, self.owner, self.project, self.stack)
        r = self.session.get(metaURL, timeout=10)
        if r.status_code != 200:
            raise ConnectionError(
                'Metadata not fetched, error {}'.format(r.reason))
        resp = r.json()
        stats = resp['stats']

        x_start = round(stats['stackBounds']['minX'])
        x_stop = round(stats['stackBounds']['maxX'])
        y_start = round(stats['stackBounds']['minY'])
        y_stop = round(stats['stackBounds']['maxY'])
        z_start = round(stats['stackBounds']['minZ'])
        z_stop = round(stats['stackBounds']['maxZ'])

        self.x_rng_unscaled = [x_start, x_stop]
        self.y_rng_unscaled = [y_start, y_stop]

        self.x_rng = [round(a * self.scale) for a in self.x_rng_unscaled]
        self.y_rng = [round(a * self.scale) for a in self.y_rng_unscaled]
        self.z_rng = [z_start, z_stop]

        self.tile_width = round(stats['maxTileWidth'])
        self.tile_height = round(stats['maxTileHeight'])

    def get_render_tile(self, z, x, y, x_width, y_width, window=None, attempts=5):
        # note that this returns data at scaled resolution, from box coords of unscaled res

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        imgURL = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale)
        if window is not None:
            imgURL += '?minIntesnity={}&maxIntensity={}'.format(
                window[0], window[1])
        for attempt in range(attempts):
            try:
                r = self.session.get(imgURL, timeout=10)
                if r.status_code != 200:
                    raise ConnectionError(
                        'Data not fetched with error: {}'.format(r.reason))
            except ConnectionError:
                if attempt != attempts - 1:
                    time.sleep(2**(attempt + 1))
            else:
                break
        else:
            # we failed all the attempts - deal with the consequences.
            raise ConnectionError(
                'Data not fetched with error: {}'.format(r.reason))

        im_obj = io.BytesIO(r.content)
        return np.array(Image.open(im_obj))[:, :, 0]

    def get_render_img(self, z, dtype='uint8', window=None, threads=8):
        # this requests the entire slice and returns the data, scaled if necessary

        # we'll break apart our request into a series of tiles
        # these will extend past the extent of the underlying data
        stride_width = 2048
        stride_height = 2048
        x_buckets = get_supercubes(
            self.x_rng_unscaled, stride=stride_width)
        y_buckets = get_supercubes(
            self.y_rng_unscaled, stride=stride_height)

        # assembling the args for each of our separate requests
        # requests are set at the unscaled full size resolution
        args = []
        for _, x in x_buckets.items():
            for _, y in y_buckets.items():
                args.append(
                    (z, x[0], y[0], stride_width, stride_height, window))

        # firing off the requests
        pool = ThreadPool(threads)
        with self.session:
            data_array = pool.starmap(self.get_render_tile, args)

        # initialize to the size of the return data (scaled if necessary)
        im_array = np.zeros([stride_height * len(y_buckets),
                             stride_width * len(x_buckets)], dtype=dtype)

        # assembling the data
        for idx, data in enumerate(data_array):
            _, x, y, x_width, y_width, _ = args[idx]
            # have to scale the box to fit the data inside
            x, y = [
                round(a * self.scale) for a in [x, y]]
            y_width, x_width = data.shape
            im_array[y - self.y_rng[0]:y - self.y_rng[0] + y_width,
                     x - self.x_rng[0]: x - self.x_rng[0] + x_width] = data

        # we finally clip the data to the bounds of the scaled data (while dealing with offsets)
        im_array = im_array[self.y_rng[0] - self.y_rng[0]:self.y_rng[1] - self.y_rng[0],
                            self.x_rng[0] - self.x_rng[0]:self.x_rng[1] - self.x_rng[0]]
        return im_array


def get_supercubes(rng, stride=512):
    # boss is stored in 512x512x16 so slice the data first and get cutouts in those ranges
    first = rng[0]    # inclusive
    last = rng[1]     # exclusive

    buckets = defaultdict(list)
    for z in range(first, last):
        buckets[(z // stride)].append(z)

    return buckets


def benchmark_get_tile(renderObj, step_size):
    times = []
    for z in range(0, 5):
        x = random.randint(renderObj.x_rng[0], renderObj.x_rng[1] - step_size)
        y = random.randint(renderObj.y_rng[0], renderObj.y_rng[1] - step_size)
        t0 = time.time()
        data = renderObj.get_render_tile(
            z, x, y, step_size, step_size, 1, window=[0, 10000])
        t1 = time.time()
        times.append((t1 - t0) / data.size * 10e5)
    tot_time = np.mean(times)
    print('Step {} took average {:.2f} sec / 10e5 pixels.'.format(step_size, tot_time))


def benchmark_get_img(renderObj, threads, num_runs=10):
    times = []
    for _ in range(0, num_runs):
        z = random.randint(renderObj.z_rng[0], renderObj.z_rng[1])
        t0 = time.time()
        im_array = renderObj.get_render_img(z, scale=1, threads=threads)
        t1 = time.time()
        times.append((t1 - t0))
    tot_time = np.mean(times)
    print('{:2d} threads took {:.2f} sec (avg of {} runs) to extract image of size {}.'.format(
        threads, tot_time, num_runs, im_array.shape))
