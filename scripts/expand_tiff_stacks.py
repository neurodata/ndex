from pathlib import Path
import argparse
import os

import numpy as np
import tifffile as tiff
from tqdm import tqdm
# from PIL import Image, ImageSequence


def parse_args():
    parser = argparse.ArgumentParser(
        description='Splits a tiff stack into separate tiff files')

    parser.add_argument('--tiffstack', type=str,
                        help='Full file path of tiff stack input')
    parser.add_argument('--outpath', type=str,
                        help='Full path for output files')
    parser.add_argument('--datatype', type=str,
                        help='Cast images as a particular dtype (uint8/uint16/uint64)')
    # parser.add_argument('--scale_to_datatype', action='store_true',
    #                     help='flag to scale the input datatype to the datatype specified')

    args = parser.parse_args()

    # validate args
    if args.tiffstack is None:
        raise ValueError('No input file')

    return args


def expand_stack(args):
    stackfile = Path(args.tiffstack)

    if args.outpath is None:
        outname = stackfile.parent / stackfile.stem
    else:
        outname = Path(args.outpath)
    if not outname.exists():
        outname.mkdir()

    stack = tiff.imread(str(stackfile))

    num_slices = len(stack)
    digits = len(str(abs(num_slices)))
    outfname = '{0:0{1}d}'

    for idx, page in tqdm(enumerate(stack), total=num_slices):
        numpypage = np.array(page, dtype=args.datatype)
        tiff.imsave(
            str(outname / (outfname.format(idx, digits) + '.tif')),
            data=numpypage)


def main():
    args = parse_args()

    expand_stack(args)


if __name__ == '__main__':
    main()
