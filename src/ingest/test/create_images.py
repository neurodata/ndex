import math
import os

import numpy as np
import png
import tifffile as tiff


def create_img_file(x_size, y_size, dtype, file_format, img_fname, intensity_range=None):
    if intensity_range is None:
        bit_width = int(''.join(filter(str.isdigit, dtype)))
    else:
        bit_width = round(math.log(intensity_range, 2))
    ar = np.random.randint(
        1, 2**bit_width, size=(y_size, x_size), dtype=dtype)

    directory = os.path.dirname(img_fname)
    if not os.path.isdir(directory):
        os.makedirs(directory)

    if file_format == 'tif':
        tiff.imsave(img_fname, ar)
    elif file_format == 'png':
        with open(img_fname, 'wb') as f:
            writer = png.Writer(width=x_size, height=y_size,
                                bitdepth=bit_width, greyscale=True)
            writer.write(f, ar.tolist())


def gen_images(ingest_job, intensity_range=None):
    for z in range(ingest_job.z_range[0], ingest_job.z_range[1], ingest_job.z_step):
        img_fname = ingest_job.get_img_fname(z)
        create_img_file(ingest_job.img_size[0], ingest_job.img_size[1],
                        ingest_job.datatype, ingest_job.extension, img_fname, intensity_range)


def del_test_images(ingest_job):
    for z in range(ingest_job.z_range[0], ingest_job.z_range[1], ingest_job.z_step):
        img_fname = ingest_job.get_img_fname(z)
        os.remove(img_fname)
