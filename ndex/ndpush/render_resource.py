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
    def __init__(
        self,
        owner,
        project,
        stack,
        baseURL,
        datatype,
        channel=None,
        scale=None,
        limit_x=None,
        limit_y=None,
        limit_z=None,
    ):
        self.owner = owner
        self.project = project
        self.stack = stack
        self.baseURL = baseURL
        if scale is None:
            self.scale = 1
        else:
            self.scale = scale

        self.datatype = datatype

        # self.level = math.log(1 / scale, 2)
        self.session = requests.Session()

        self.limit_x = limit_x
        self.limit_y = limit_y
        self.limit_z = limit_z

        self.set_metadata()

        if channel:
            assert channel in self.channel_names

        # defaults to None
        self.channel = channel

    def __str__(self):
        from pprint import pformat

        return "<" + type(self).__name__ + "> " + pformat(vars(self), indent=4, width=1)

    def get_render_img(self, z, window=None, threads=1, tile_size=8192):
        # this requests the entire slice and returns the data, scaled if necessary

        # we'll break apart our request into a series of tiles
        # these will extend past the extent of the underlying data
        stride = round(tile_size / self.scale)  # 8K
        x_buckets = get_supercubes(self.x_rng_unscaled, stride=stride)
        y_buckets = get_supercubes(self.y_rng_unscaled, stride=stride)

        # assembling the args for each of our separate requests
        # requests are set at the unscaled full size resolution
        args = []
        for _, x in x_buckets.items():
            for _, y in y_buckets.items():
                args.append(
                    (
                        z,
                        x[0],
                        y[0],
                        min(stride, x[-1] - x[0] + 1),
                        min(stride, y[-1] - y[0] + 1),
                        window,
                    )
                )

        # firing off the requests
        with ThreadPool(threads) as pool:
            data_array = pool.starmap(self.get_render_tile, args)

        # initialize to the size of the return data (scaled if necessary)
        im_array = np.zeros(
            [tile_size * len(y_buckets), tile_size * len(x_buckets)],
            dtype=self.datatype,
        )

        # assembling the data
        for idx, data in enumerate(data_array):
            _, x, y, x_width, y_width, _ = args[idx]
            # have to scale the box to fit the data inside
            x_s, y_s = [round(a * self.scale) for a in [x, y]]
            y_width, x_width = data.shape
            im_array[
                y_s - self.y_rng[0] : y_s - self.y_rng[0] + y_width,
                x_s - self.x_rng[0] : x_s - self.x_rng[0] + x_width,
            ] = data

        # we finally clip the data to the bounds of the scaled data (while dealing with offsets)
        im_array = im_array[
            0 : self.y_rng[1] - self.y_rng[0], 0 : self.x_rng[1] - self.x_rng[0]
        ]
        return im_array

    def set_metadata(self):
        # even if you have a channel the metadata is located at the stack level
        metaURL = "{}owner/{}/project/{}/stack/{}".format(
            self.baseURL, self.owner, self.project, self.stack
        )
        r = self.session.get(metaURL, timeout=10)
        if r.status_code != 200:
            raise ConnectionError("Metadata not fetched, error {}".format(r.reason))
        resp = r.json()
        stats = resp["stats"]

        x_start = round(stats["stackBounds"]["minX"])
        x_stop = round(stats["stackBounds"]["maxX"])
        y_start = round(stats["stackBounds"]["minY"])
        y_stop = round(stats["stackBounds"]["maxY"])
        z_start = round(stats["stackBounds"]["minZ"])
        z_stop = round(stats["stackBounds"]["maxZ"])

        self.x_rng_unscaled = [x_start, x_stop]
        self.y_rng_unscaled = [y_start, y_stop]

        self.z_rng = [z_start, z_stop]

        self.validate_xyz_limits()
        self.apply_limits()

        self.x_rng = [round(a * self.scale) for a in self.x_rng_unscaled]
        self.y_rng = [round(a * self.scale) for a in self.y_rng_unscaled]

        self.tile_width = round(stats["maxTileWidth"])
        self.tile_height = round(stats["maxTileHeight"])

        channels = stats["channelNames"]
        self.channel_names = channels

    def apply_limits(self):
        if self.limit_x is not None:
            self.x_rng_unscaled = self.limit_x
        if self.limit_y is not None:
            self.y_rng_unscaled = self.limit_y
        if self.limit_z is not None:
            self.z_rng = self.limit_z

    def validate_xyz_limits(self):
        validate_limit(self.x_rng_unscaled, self.limit_x)
        validate_limit(self.y_rng_unscaled, self.limit_y)
        validate_limit(self.z_rng, self.limit_z)

    def gen_render_url(self, z, x, y, x_width, y_width, window=None):
        # using the box cutout

        # this gives you the data at certain levels of downsampling (0 = full res, 1 = half, etc)
        # row and column are the number of rows/colums in the data with the specified width/height
        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/largeDataTileSource/{width}/{height}/{level}/{z}/{row}/{column}.png

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/tiff16-image
        if self.datatype == "uint16":
            img_type = "tiff16"
        else:
            img_type = "png"

        img_URL = "{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/{}-image".format(
            self.baseURL,
            self.owner,
            self.project,
            self.stack,
            z,
            x,
            y,
            x_width,
            y_width,
            self.scale,
            img_type,
        )

        params = []
        if self.channel is not None:
            params.append("channels={}".format(self.channel))

        if window is not None:
            params.append(
                "minIntensity={}&maxIntensity={}".format(window[0], window[1])
            )

        if params:
            img_URL += "?" + "&".join(params)

        return img_URL

    def get_render_tile(self, z, x, y, x_width, y_width, window=None, attempts=6):
        # note that this returns data at scaled resolution, from box coords of unscaled res
        img_URL = self.gen_render_url(z, x, y, x_width, y_width, window=window)

        for attempt in range(attempts):
            try:
                r = self.session.get(img_URL, timeout=60)
                if r.status_code != 200:
                    raise ConnectionError(
                        "Data not fetched with error: {}".format(r.reason)
                    )
            except Exception:
                if attempt != attempts - 1:
                    time.sleep(2 ** (attempt + 1))
            else:
                break
        else:
            # we failed all the attempts - deal with the consequences.
            raise ConnectionError(
                "Data from URL {} not fetched.  Status code {}, error {}".format(
                    img_URL, r.status_code, r.reason
                )
            )

        im_obj = io.BytesIO(r.content)

        if self.datatype == "uint16":
            return np.array(Image.open(im_obj))
        else:
            return np.array(Image.open(im_obj))[:, :, 0]


def validate_limit(data_rng, limit):
    if limit is not None:
        if limit[0] < data_rng[0] or limit[1] > data_rng[1]:
            raise ValueError


def get_supercubes(rng, stride=512):
    # boss is stored in 512x512x16 so slice the data first and get cutouts in those ranges
    first = rng[0]  # inclusive
    last = rng[1]  # exclusive

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
            z, x, y, step_size, step_size, 1, window=[0, 10000]
        )
        t1 = time.time()
        times.append((t1 - t0) / data.size * 10e5)
    tot_time = np.mean(times)
    print("Step {} took average {:.2f} sec / 10e5 pixels.".format(step_size, tot_time))


def benchmark_get_img(renderObj, threads, num_runs=10):
    times = []
    for _ in range(0, num_runs):
        z = random.randint(renderObj.z_rng[0], renderObj.z_rng[1])
        t0 = time.time()
        im_array = renderObj.get_render_img(z, scale=1, threads=threads)
        t1 = time.time()
        times.append((t1 - t0))
    tot_time = np.mean(times)
    print(
        "{:2d} threads took {:.2f} sec (avg of {} runs) to extract image of size {}.".format(
            threads, tot_time, num_runs, im_array.shape
        )
    )

