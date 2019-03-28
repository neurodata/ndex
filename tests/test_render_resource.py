from io import BytesIO

import numpy as np
import pytest
import requests
from PIL import Image

from ndex.ndpush.render_resource import renderResource

try:
    r = requests.get("http://render-dev-eric.neurodata.io")
    skip_tests = False
except requests.exceptions.ConnectionError:
    skip_tests = True


@pytest.mark.skipif(skip_tests, reason="render-dev not responding")
class TestRenderResource:
    def setup_method(self):
        self.owner = "Forrest"
        self.project = "M247514_Rorb_1"
        self.stack = "Take2Site5Align_Session3"
        self.scale = 1
        self.baseURL = "http://render-dev-eric.neurodata.io/render-ws/v1/"
        self.datatype = "uint8"

        self.x_rng = [-52178, 34908]
        self.y_rng = [-56168, 78283]
        self.z_rng = [0, 50]

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_create_render_resource(self):
        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            scale=self.scale,
        )

        assert render_obj.x_rng == self.x_rng
        assert render_obj.y_rng == self.y_rng
        assert render_obj.z_rng == self.z_rng
        assert render_obj.datatype == "uint8"

    def setup_render_channel(self):
        self.owner = "Forrest"
        self.project = "M247514_Rorb_1"
        self.stack = "Site3Align2_LENS_Session1"
        self.channel = "DAPI1"

    def test_create_render_resource_channel(self):
        # metadata:
        # http://render-dev-eric.neurodata.io/render-ws/v1/owner/Forrest/project/M247514_Rorb_1/stack/Site3Align2_LENS_Session1
        self.setup_render_channel()
        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            channel=self.channel,
            scale=self.scale,
        )

        assert render_obj.x_rng == [-27814, 63397]
        assert render_obj.y_rng == [-67750, 69699]
        assert render_obj.z_rng == [0, 50]

    def test_create_render_resource_wrong_channel(self):
        owner = "Forrest"
        project = "M247514_Rorb_1"
        stack = "Site3Align2_LENS_Session1"
        channel = "notAchannel"

        with pytest.raises(AssertionError):
            renderResource(
                owner,
                project,
                stack,
                self.baseURL,
                self.datatype,
                channel=channel,
                scale=self.scale,
            )

    def test_wrong_stack(self):
        owner = "6_ribbon_experiments"
        project = "M321160_Ai139_smallvol"
        stack = "DOES_NOT_EXIST"
        with pytest.raises(ConnectionError):
            renderResource(owner, project, stack, self.baseURL, self.datatype)

    def test_create_render_resource_half_scale(self):
        self.scale = 0.5
        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            scale=self.scale,
        )

        assert render_obj.scale == self.scale

        assert render_obj.x_rng == [round(x * self.scale) for x in self.x_rng]
        assert render_obj.y_rng == [round(x * self.scale) for x in self.y_rng]
        assert render_obj.z_rng == self.z_rng

    def test_get_render_tile_no_window(self):
        x = 1024
        y = 512
        z = 17
        x_width = 512
        y_width = 1024

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = "{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image".format(
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
        )
        print(tile_url)
        r = requests.get(tile_url)

        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            scale=self.scale,
        )
        data = render_obj.get_render_tile(z, x, y, x_width, y_width)

        assert data.shape == (y_width, x_width)
        assert np.array_equal(data, test_data)

    def test_get_render_tile_no_window_uint16(self):
        x = 50
        y = 512
        z = 17
        x_width = 512
        y_width = 1024
        datatype = "uint16"

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/tiff16-image
        tile_url = "{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/tiff16-image".format(
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
        )
        print(tile_url)
        r = requests.get(tile_url)

        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)

        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            datatype,
            scale=self.scale,
        )
        data = render_obj.get_render_tile(z, x, y, x_width, y_width)

        assert data.shape == (y_width, x_width)
        assert np.array_equal(data, test_data)

    def test_get_render_tile(self):
        x = 1024
        y = 512
        z = 17
        x_width = 512
        y_width = 1024
        window = [0, 10000]

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = "{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntensity={}&maxIntensity={}".format(
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
            window[0],
            window[1],
        )
        r = requests.get(tile_url)
        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            scale=self.scale,
        )
        data = render_obj.get_render_tile(z, x, y, x_width, y_width, window)

        assert data.shape == (y_width, x_width)
        assert np.array_equal(data, test_data)

    def test_get_render_tile_channel_scale(self):
        self.setup_render_channel()
        self.scale = 0.125
        render_obj = renderResource(
            self.owner,
            self.project,
            self.stack,
            self.baseURL,
            self.datatype,
            channel=self.channel,
            scale=self.scale,
        )

        x = 4200
        y = 6500
        z = 24
        x_width = 1024
        y_width = 1024
        window = [0, 5000]

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = "{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?channels={}&minIntensity={}&maxIntensity={}".format(
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
            self.channel,
            window[0],
            window[1],
        )

        r = requests.get(tile_url)
        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        # getting tile from render resource
        data = render_obj.get_render_tile(z, x, y, x_width, y_width, window)

        assert data.shape == (y_width * self.scale, x_width * self.scale)
        assert np.array_equal(data, test_data)
