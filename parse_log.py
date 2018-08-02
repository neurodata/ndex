import argparse
import os

from tqdm import tqdm


def get_nonexistant_path(fname_path):
    """
    Get the path to a filename which does not exist by incrementing path.

    Examples
    --------
    >>> get_nonexistant_path('/etc/issue')
    '/etc/issue-1'
    >>> get_nonexistant_path('whatever/1337bla.py')
    'whatever/1337bla.py'
    """
    if not os.path.exists(fname_path):
        return fname_path
    filename, file_extension = os.path.splitext(fname_path)
    i = 1
    new_fname = "{}-{}{}".format(filename, i, file_extension)
    while os.path.exists(new_fname):
        i += 1
        new_fname = "{}-{}{}".format(filename, i, file_extension)
    return new_fname


def search(lines, text):
    results = []
    for line in lines:
        if text in line:
            results.append(line)
    return results


def parse_log(logfile, outfile):
    # parse the log file to generate the repeat_cutouts file

    if os.path.isfile(outfile):
        outfile = get_nonexistant_path(outfile)

    # this just creates and empties the outfile if it already exists
    with open(outfile, 'w'):
        pass

    # read the entire thing into memory because we have to search the thing multiple times
    with open(logfile) as f:
        log_lines = f.readlines()

    error_lines = search(log_lines, ', skipping')
    success_lines = search(log_lines, 'POST succeeded')

    for line in tqdm(error_lines):
        line_split = line.split(' Coll: ')
        cutout = 'Coll: {}'.format(line_split[1]).strip('\n')
        cutout_lines = search(success_lines, cutout)
        if not cutout_lines:
            with open(outfile, 'a') as fo:
                fo.write(cutout + '\n')

    return outfile


def main():
    parser = argparse.ArgumentParser(description='Search log file for errors')
    parser.add_argument('--logfile', type=str,
                        default='log.txt', help='log file to parse')
    parser.add_argument('--outfile', type=str,
                        default='repeat_cutouts.txt', help='log file to parse')
    args = parser.parse_args()

    print('Parsing the log file...')
    parse_log(args.logfile, args.outfile)
    print('Parsing log file complete.')


if __name__ == '__main__':
    main()
