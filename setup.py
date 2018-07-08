from setuptools import setup

setup(name='pysparkMedoids',
      version='0.1.0',
      description='kMedoids clustering in pySpark for a user supplied distance function',
      url='https://github.com/chdhr-harshal/pysparkMedoids',
      author='Harshal A. Chaudhari',
      author_email='harshal@bu.edu',
      license='MIT',
      packages=['pysparkMedoids'],
      install_requires=[
                'scipy',
                'numpy'
          ],
      zip_safe=False)
