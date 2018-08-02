from ndpull.ndpull import *

import os


class Testndpull():

    def test_print_meta(self):
        meta = BossMeta('Zbrain', 'Zbrain', 'Anti_5HT_MeanOf40')

        token, boss_url = get_boss_config()
        rmt = BossRemote(boss_url, token, meta)
        print(rmt)

    def test_wrong_extents(self):
        args = argparse.Namespace(
            x=[0, 512],
            y=[500, 1000],
            z=[-1, 10],
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=None,
            full_extent=None,
            res=0,
            iso=False,
        )
        with pytest.raises(ValueError):
            validate_args(args)

    def test_resolution_set(self):
        res = 4
        boss_meta = BossMeta('Zbrain', 'Zbrain', 'Anti_5HT_MeanOf40', res)
        assert boss_meta.res() == res

    def test_resolution_too_high(self):
        res = 5
        args = argparse.Namespace(
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=None,
            full_extent=True,
            res=res,
            iso=False,
        )
        with pytest.raises(ValueError):
            validate_args(args)

    def test_extents(self):
        res = 0
        args = argparse.Namespace(
            x=None,
            y=None,
            z=None,
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=False,
            full_extent=True,
            res=res,
            outdir='test_images/',
            iso=False,
        )
        _, rmt = validate_args(args)
        xyz = rmt.get_xyz_extents()
        assert xyz == ([0, 1406], [0, 621], [0, 138])

    def test_extents_downsampled(self):
        res = 3
        args = argparse.Namespace(
            x=None,
            y=None,
            z=None,
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=False,
            full_extent=True,
            res=res,
            outdir='test_images/',
            iso=False,
        )
        _, rmt = validate_args(args)
        xyz = rmt.get_xyz_extents()
        assert xyz == ([0, 176], [0, 78], [0, 138])

    def test_extents_downsampled_iso(self):
        res = 3
        args = argparse.Namespace(
            x=None,
            y=None,
            z=None,
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=False,
            full_extent=True,
            res=res,
            outdir='test_images/',
            iso=True,
        )
        _, rmt = validate_args(args)
        xyz = rmt.get_xyz_extents()
        assert xyz == ([0, 176], [0, 78], [0, 17])

    def test_get_full_range_downsampled(self):
        res = 3
        args = argparse.Namespace(
            x=None,
            y=None,
            z=None,
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=False,
            full_extent=True,
            res=res,
            outdir='test_images/',
            iso=False,
        )
        result, rmt = validate_args(args)
        download_slices(result, rmt)

        # get the data from boss web api directly
        cutout_url_base = "{}/{}/cutout/{}/{}/{}".format(
            result.url, BOSS_VERSION, result.collection, result.experiment, result.channel)
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, res, result.x[0], result.x[1],
            result.y[0], result.y[1], result.z[0], result.z[1])
        resp = requests.get(cutout_url,
                            headers={'Authorization': 'Token {}'.format(result.token),
                                     'Accept': 'application/blosc'})
        resp.raise_for_status()
        data_decompress = blosc.decompress(resp.content)
        datatype = 'uint16'
        data_np = np.fromstring(data_decompress, dtype=datatype)
        data_direct = np.reshape(
            data_np, (result.z[1]-result.z[0], result.y[1]-result.y[0], result.x[1]-result.x[0]))
        for z in range(result.z[0], result.z[1]):
            tiff.imsave('test_images/{}.tif'.format(z),
                        data=data_direct[z-result.z[0], :])

        # assert here that the tiff files are equal
        for z in range(result.z[0], result.z[1]):
            data_direct = tiff.imread('test_images/{}.tif'.format(z))

            fname = 'test_images/{}_{}_{}_x{x[0]}-{x[1]}_y{y[0]}-{y[1]}_z{z:0{dig}d}.tif'.format(
                result.collection, result.experiment, result.channel,
                x=result.x, y=result.y, z=z, dig=3)
            data_pull = tiff.imread(fname)

            assert np.array_equal(data_direct, data_pull)

            os.remove('test_images/{}.tif'.format(z))
            os.remove(fname)

    def test_create_rmt(self):
        args = argparse.Namespace(
            x=[0, 512],
            y=[500, 600],
            z=[0, 10],
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=None,
            full_extent=None,
            res=0,
            iso=False,
        )
        result, rmt = validate_args(args)

        assert rmt.meta.collection() == result.collection
        assert rmt.meta.experiment() == result.experiment
        assert rmt.meta.channel() == result.channel

    def test_small_cutout(self):
        args = argparse.Namespace(
            x=[0, 512],
            y=[500, 600],
            z=[15, 22],
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=None,
            full_extent=None,
            res=0,
            iso=False,
        )
        datatype = 'uint16'  # for validation images

        result, rmt = validate_args(args)

        cutout_url_base = "{}/{}/cutout/{}/{}/{}".format(
            result.url, BOSS_VERSION, result.collection, result.experiment, result.channel)
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, 0, result.x[0], result.x[1],
            result.y[0], result.y[1], result.z[0], result.z[1])
        resp = requests.get(cutout_url,
                            headers={'Authorization': 'Token {}'.format(result.token),
                                     'Accept': 'application/blosc'})
        resp.raise_for_status()
        data_decompress = blosc.decompress(resp.content)
        data_np = np.fromstring(data_decompress, dtype=datatype)
        data_direct = np.reshape(data_np, (7, 100, 512))

        data = rmt.cutout(result.x, result.y, result.z, datatype, attempts=1)

        assert np.array_equal(data, data_direct)

    def test_small_download(self):
        args = argparse.Namespace(
            x=[81920, 81920+512],
            y=[81920, 81920+500],
            z=[3200, 3216],
            config_file=None,
            collection='bock',
            experiment='bock11',
            channel='image',
            print_metadata=None,
            full_extent=None,
            res=0,
            outdir='test_images/',
            iso=False,
        )

        result, rmt = validate_args(args)
        download_slices(result, rmt)

        # get the data from boss web api directly
        cutout_url_base = "{}/{}/cutout/{}/{}/{}".format(
            result.url, BOSS_VERSION, result.collection, result.experiment, result.channel)
        cutout_url = "{}/{}/{}:{}/{}:{}/{}:{}/".format(
            cutout_url_base, 0, result.x[0], result.x[1],
            result.y[0], result.y[1], result.z[0], result.z[1])
        resp = requests.get(cutout_url,
                            headers={'Authorization': 'Token {}'.format(result.token),
                                     'Accept': 'application/blosc'})
        resp.raise_for_status()
        data_decompress = blosc.decompress(resp.content)
        datatype = 'uint8'
        data_np = np.fromstring(data_decompress, dtype=datatype)
        data_direct = np.reshape(
            data_np, (result.z[1]-result.z[0], result.y[1]-result.y[0], result.x[1]-result.x[0]))
        for z in range(result.z[0], result.z[1]):
            tiff.imsave('test_images/{}.tif'.format(z),
                        data=data_direct[z-result.z[0], :])

        # assert here that the tiff files are equal
        for z in range(result.z[0], result.z[1]):
            data_direct = tiff.imread('test_images/{}.tif'.format(z))

            fname = 'test_images/{}_{}_{}_x{x[0]}-{x[1]}_y{y[0]}-{y[1]}_z{z:0{dig}d}.tif'.format(
                result.collection, result.experiment, result.channel,
                x=result.x, y=result.y, z=z, dig=3)
            data_pull = tiff.imread(fname)

            assert np.array_equal(data_direct, data_pull)

            os.remove('test_images/{}.tif'.format(z))
            os.remove(fname)

    def test_stack_gen(self):

        args = argparse.Namespace(
            x=[0, 512],
            y=[500, 600],
            z=[15, 22],
            config_file=None,
            collection='Zbrain',
            experiment='Zbrain',
            channel='Anti_5HT_MeanOf40',
            print_metadata=None,
            full_extent=None,
            res=0,
            iso=False,
            outdir='test_images/',

            stack_filename='stackfile_test.tif',
        )
        result, rmt = validate_args(args)
        download_slices(result, rmt)
        save_to_stack(rmt.meta, result)

        stack_fname = Path(result.outdir, result.stack_filename)

        # test to see if it's the same # of pages
        with tiff.TiffFile(str(stack_fname)) as tif:
            assert len(tif.pages) == result.z[1]-result.z[0]

        stack_fname.unlink()
