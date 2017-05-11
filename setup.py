from setuptools import setup


setup(
    name='tztz',
    version='0.1.0',
    description='Toolz for Toolz',
    author='Christopher Prohm',
    author_email='mail@cprohm.de',
    license='MIT',
    packages=["tztz"],
    tests_require=['pytest', 'dask', 'pytest-pep8'],
    classifiers=[
        'Development Status :: 4 - Beta',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
)
