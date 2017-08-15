import argparse

#not needed in newer log files (could be extracted from line)
# COLL_NAME = 'weiler14'

def parse(line_str, log_lines_all):
    if 'failed to download cutout, skipping' in line_str or 'Error: data upload failed after multiple attempts' in line_str:
        #need a test here to see if there is a valid POST for the same cutout region
        
        line_split = line_str.split(' Coll: ')
        cutout = 'Coll: {}'.format(line_split[1])
        
        log_lines_all.remove(line_str)

        indices = [i for i, s in enumerate(log_lines_all) if cutout in s]
        for ii in indices:
            if 'POST success' in log_lines_all[ii]:
                return 1

        with open(OUTFILE, 'a') as fo:
            fo.writelines(cutout)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search log file for errors')

    parser.add_argument('--logfile', type=str, default='log.txt', help='log file to parse')

    parser.add_argument('--outfile', type=str, default='repeat_cutouts.txt', help='log file to parse')

    args = parser.parse_args()

    LOGFILE = args.logfile
    OUTFILE = args.outfile

    with open(OUTFILE, 'w') as fo: pass

    with open(LOGFILE) as f:
        log_lines = f.readlines()

    log_lines = sorted(set(log_lines))

    for line in log_lines:
        parse(line, log_lines)
