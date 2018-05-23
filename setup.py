from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))
version = '0.2.19'
modules = [
    'aiohttp>=2.3.6,<3.0'
]

setup(
    name='aiokts',
    version=version,
    description='Tuned asyncio and aiohttp classes for simpler creation of powerful APIs',
    long_description='Tuned asyncio and aiohttp classes for simpler creation of powerful APIs',

    author='KTS',
    author_email='aiokts@ktsstudio.ru',
    url='https://github.com/KTSStudio/aiokts',
    download_url='https://github.com/KTSStudio/aiokts/tarball/v' + version,
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    keywords=' '.join(['aiohttp', 'asyncio', 'python 3.5', 'api']),
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=modules,
)
