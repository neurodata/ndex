from io import BytesIO

import numpy as np
import pytest
import requests
from PIL import Image

from ..render_resource import renderResource


class TestRenderResource:
    def setup_method(self):
        self.owner = '6_ribbon_experiments'
        self.project = 'M321160_Ai139_smallvol'
        # stack = 'Acquisition_1_PSD95' #10kx10k
        self.stack = 'Median_1_GFP'
        self.scale = 1
        self.baseURL = 'https://render-dev-eric.neurodata.io/render-ws/v1/'

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_create_render_resource(self):
        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        assert render_obj.x_rng == [0, 5608]
        assert render_obj.y_rng == [0, 2049]
        assert render_obj.z_rng == [0, 536]
        assert render_obj.tile_width == 2048
        assert render_obj.tile_width == 2048

    def setup_render_channel(self):
        self.owner = 'Forrest'
        self.project = 'M247514_Rorb_1'
        self.stack = 'Site3Align2_LENS_Session1'
        self.channel = 'DAPI1'

    def test_create_render_resource_channel(self):
        # metadata:
        # http://render-dev-eric.neurodata.io/render-ws/v1/owner/Forrest/project/M247514_Rorb_1/stack/Site3Align2_LENS_Session1
        self.setup_render_channel()
        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, channel=self.channel, scale=self.scale)

        assert render_obj.x_rng == [-27814, 63396]
        assert render_obj.y_rng == [-67750, 69698]
        assert render_obj.z_rng == [0, 49]

    def test_create_render_resource_wrong_channel(self):
        owner = 'Forrest'
        project = 'M247514_Rorb_1'
        stack = 'Site3Align2_LENS_Session1'
        channel = 'notAchannel'

        with pytest.raises(AssertionError):
            renderResource(owner, project, stack, self.baseURL,
                           channel=channel, scale=self.scale)

    def test_wrong_stack(self):
        owner = '6_ribbon_experiments'
        project = 'M321160_Ai139_smallvol'
        stack = 'DOES_NOT_EXIST'
        with pytest.raises(ConnectionError):
            renderResource(owner, project, stack, self.baseURL)

    def test_create_render_resource_half_scale(self):
        self.scale = .5
        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        assert render_obj.scale == self.scale

        assert render_obj.x_rng == [0, 2804]
        assert render_obj.y_rng == [0, 1024]
        assert render_obj.z_rng == [0, 536]
        assert render_obj.tile_width == 2048
        assert render_obj.tile_width == 2048

    def test_get_render_tile_no_window(self):
        x = 1024
        y = 512
        z = 17
        x_width = 512
        y_width = 1024

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale)
        print(tile_url)
        r = requests.get(tile_url)

        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)
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
        tile_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntensity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale, window[0], window[1])
        r = requests.get(tile_url)
        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)
        data = render_obj.get_render_tile(
            z, x, y, x_width, y_width, window)

        assert data.shape == (y_width, x_width)
        assert np.array_equal(data, test_data)

    def test_get_render_tile_channel_scale(self):
        self.setup_render_channel()
        self.scale = .125
        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, channel=self.channel, scale=self.scale)

        x = 4200
        y = 6500
        z = 24
        x_width = 1024
        y_width = 1024
        window = [0, 5000]

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?channel={}&minIntensity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale, self.channel, window[0], window[1])

        r = requests.get(tile_url)
        test_img = Image.open(BytesIO(r.content))
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        # getting tile from render resource
        data = render_obj.get_render_tile(
            z, x, y, x_width, y_width, window)

        assert data.shape == (y_width * self.scale, x_width * self.scale)
        assert np.array_equal(data, test_data)

    def test_get_render_img(self):
        z = 200
        window = [0, 5000]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntensity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z,
            render_obj.x_rng[0], render_obj.y_rng[0],
            render_obj.x_rng[1], render_obj.y_rng[1],
            self.scale, window[0], window[1])
        r = requests.get(render_url)
        test_img = Image.open(BytesIO(r.content))
        test_data = np.asarray(test_img)[:, :, 0]

        data = render_obj.get_render_img(z, dtype='uint8', window=window)

        assert np.array_equal(data, test_data)

    # def test_get_render_scaled_tile_channel(self):
    #     self.scale = .125
    #     z = 20
    #     window = [0, 5000]

    #     self.setup_render_channel()
    #     render_obj = renderResource(
    #         self.owner, self.project, self.stack, self.baseURL, channel=self.channel, scale=self.scale)

    #     data = render_obj.get_render_img(z, dtype='uint8', window=window)

    #     render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?channel={}&minIntensity={}&maxIntensity={}'.format(
    #         self.baseURL, self.owner, self.project, self.stack, z,
    #         render_obj.x_rng_unscaled[0], render_obj.y_rng_unscaled[0],
    #         sum(map(abs, render_obj.x_rng_unscaled)),
    #         sum(map(abs, render_obj.y_rng_unscaled)),
    #         self.scale, self.channel, window[0], window[1])

    #     print(render_url)

    #     r = requests.get(render_url, timeout=15)
    #     test_img = Image.open(BytesIO(r.content))
    #     test_data = np.asarray(test_img)[:, :, 0]

    #     # for comparison:
    #     # test_img_fn = 'local_img_test_data\\render_img_test_scale_channel.png'
    #     # test_img = Image.fromarray(test_data)
    #     # test_img.save(test_img_fn)

    #     # rend_img = Image.fromarray(data)
    #     # rend_img.save(test_img_fn[0:-4] + '_rend_res.png')

    #     assert data.shape == test_data.shape
    #     assert np.sum(data) == np.sum(test_data)
    #     assert np.array_equal(data, test_data)

    def test_get_render_scaled_img(self):
        self.scale = .25
        z = 200
        window = [0, 5000]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntensity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z,
            render_obj.x_rng_unscaled[0], render_obj.y_rng_unscaled[0],
            render_obj.x_rng_unscaled[1], render_obj.y_rng_unscaled[1],
            self.scale, window[0], window[1])
        r = requests.get(render_url)

        test_img = Image.open(BytesIO(r.content))
        test_data = np.asarray(test_img)[:, :, 0]

        data = render_obj.get_render_img(z, dtype='uint8', window=window)

        assert data.shape == test_data.shape
        assert np.array_equal(data, test_data)

    def test_get_render_wrong_img(self):
        z = 200
        window = [0, 5000]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)
        data = render_obj.get_render_img(z, dtype='uint8', window=window)

        window = [0, 10000]
        render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntensity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z,
            render_obj.x_rng[0], render_obj.y_rng[0],
            render_obj.x_rng[1], render_obj.y_rng[1],
            self.scale, window[0], window[1])
        r = requests.get(render_url)
        test_img = Image.open(BytesIO(r.content))
        test_data = np.asarray(test_img)[:, :, 0]

        assert not np.array_equal(data, test_data)
