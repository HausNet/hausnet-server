##
# Setup. For testing on PyPi test, use:
#    pip install --extra-index-url https://testpypi.python.org/pypi hausnet-server
# to avoid dependencies not being found.

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hausnet_server",
    version="0.0.2",
    author="Louis Calitz",
    author_email="louis@hausnet.io",
    description="A server for the HausNet protocol",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/liber-tas/hausmon-client",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'paho-mqtt',
        'aioreactive',
        'janus',
    ]
)
