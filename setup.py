import os

from setuptools import find_packages, setup

import ndpull

here = os.path.abspath(os.path.dirname(__file__))

# get the dependencies and installs
with open(os.path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]

setup(name='ndpull',
      version=ndpull.version,
      description='Python 3 program to download data from NeuroData',
      url='https://github.com/neurodata/ndpull',
      author='Benjamin Falk',
      author_email='falk.ben@jhu.edu',
      license='Apache 2.0',
      packages=find_packages(),
      install_requires=install_requires,
      keywords=[
          'brain',
          'microscopy',
          'neuroscience',
          'connectome',
          'connectomics',
          'spatial',
          'EM',
          'electron',
          'calcium',
          'database',
          'boss'
      ],
      zip_safe=False,
      entry_points={'console_scripts':
                    ['ndpull=ndpull.ndpull:main'], },
      )
