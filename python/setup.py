#!/usr/bin/env python3


from setuptools import setup

setup(name='databusclient',
      version='0.1',
      description='A simple client for submitting data to the databus',
      url='https://github.com/dbpedia/databus-client/python',
      author='DBpedia Association',
      author_email='',
      license='Apache-2.0 License',
      packages=['databusclient'],
      install_requires=[
            "argparse-prompt==0.0.5",
            "requests==2.27.1",
            "urllib3==1.26.8",
            "typer==0.4.1"
      ],
      zip_safe=False,
      entry_points={
        "console_scripts": [
            "databusclient" "=databusclient.__main__:main"
        ]
      }
    )
