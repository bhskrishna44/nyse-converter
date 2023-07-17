from setuptools import setup

setup(name='nyse-converter',
      version='0.1',
      description='NYSE data converter',
      url='https://github.com/bhskrishna44/nyse-converter',
      author='Sanjay Bhupathiraju',
      author_email='bh.skrishna@gmail.com',
      license='MIT',
      packages=['nyse_converter'],
      zip_safe = False,
      install_requires=[
          'pandas <= 1.5.10',
      ],
      entry_points = {
          'console_scripts' : ['nyse=nyse_converter:main']
      }
      )