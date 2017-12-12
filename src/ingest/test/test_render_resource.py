import os

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

    def test_broken_resource(self):
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
        test_img_fn = 'local_img_test_data\\render_tile_no_window.png'

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale)
        print(tile_url)
        r = requests.get(tile_url)
        with open(test_img_fn, "wb") as file:
            file.write(r.content)

        test_img = Image.open(test_img_fn)
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
        test_img_fn = 'local_img_test_data\\render_tile.png'

        # GET /v1/owner/{owner}/project/{project}/stack/{stack}/z/{z}/box/{x},{y},{width},{height},{scale}/png-image
        tile_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntesnity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z, x, y, x_width, y_width, self.scale, window[0], window[1])
        r = requests.get(tile_url)
        with open(test_img_fn, "wb") as file:
            file.write(r.content)

        test_img = Image.open(test_img_fn)
        # dim 3 is RGBA (A=alpha), for grayscale, RGB values are all the same
        test_data = np.asarray(test_img)[:, :, 0]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)
        data = render_obj.get_render_tile(
            z, x, y, x_width, y_width, window)

        assert data.shape == (y_width, x_width)
        assert np.array_equal(data, test_data)

    def test_get_render_img(self):
        test_img_fn = 'local_img_test_data\\render_img_test.png'
        z = 200
        window = [0, 5000]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntesnity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z,
            render_obj.x_rng[0], render_obj.y_rng[0],
            render_obj.x_rng[1], render_obj.y_rng[1],
            self.scale, window[0], window[1])
        r = requests.get(render_url)
        with open(test_img_fn, "wb") as file:
            file.write(r.content)

        test_img = Image.open(test_img_fn)
        test_data = np.asarray(test_img)[:, :, 0]

        data = render_obj.get_render_img(
            z, dtype='uint8', window=window, threads=8)

        img_render_data = Image.fromarray(data)
        img_render_data.save(
            'local_img_test_data\\render_img_test_test.png')

        assert np.array_equal(data, test_data)

    def test_get_render_scaled_img(self):
        test_img_fn = 'local_img_test_data\\render_img_test_scale.png'
        self.scale = .25
        z = 200
        window = [0, 5000]

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)

        render_url = '{}owner/{}/project/{}/stack/{}/z/{}/box/{},{},{},{},{}/png-image?minIntesnity={}&maxIntensity={}'.format(
            self.baseURL, self.owner, self.project, self.stack, z,
            render_obj.x_rng_unscaled[0], render_obj.y_rng_unscaled[0],
            render_obj.x_rng_unscaled[1], render_obj.y_rng_unscaled[1],
            self.scale, window[0], window[1])
        r = requests.get(render_url)
        with open(test_img_fn, "wb") as file:
            file.write(r.content)

        test_img = Image.open(test_img_fn)
        test_data = np.asarray(test_img)[:, :, 0]

        data = render_obj.get_render_img(
            z, dtype='uint8', window=window, threads=8)

        img_render_data = Image.fromarray(data)
        img_render_data.save(
            'local_img_test_data\\render_img_test_scale_test.png')

        assert data.shape == test_data.shape
        assert np.array_equal(data, test_data)

    def test_get_render_wrong_img(self):
        test_img_fn = 'local_img_test_data\\render_img_test.png'
        test_img = Image.open(test_img_fn)
        test_data = np.asarray(test_img)[:, :, 0]

        z = 100

        render_obj = renderResource(
            self.owner, self.project, self.stack, self.baseURL, scale=self.scale)
        data = render_obj.get_render_img(
            z, dtype='uint8', window=[0, 5000], threads=8)

        assert not np.array_equal(data, test_data)
