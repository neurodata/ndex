import os

from setuptools import find_packages, setup

import ndex

here = os.path.abspath(os.path.dirname(__file__))

# get the dependencies and installs
with open(os.path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]

setup(name='ndexchange',
      version=ndex.version,
      description='Python 3 program to download data from NeuroData',
      url='https://github.com/neurodata/ndex',
      author='Benjamin Falk',
      author_email='falk.ben@jhu.edu',
      license='Apache 2.0',
      packages=find_packages(),
      install_requires=install_requires,
      python_requires=">=3.5",
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
                    ['ndpull=ndex.ndpull.ndpull:main',
                     'ndpush=ndex.ndpush.ingest_large_vol:main',
                     'expand_stacks=scripts.expand_stacks:main',
                     ], },
      )
