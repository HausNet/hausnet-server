##
# Setup. For testing on PyPi test, use:
#    pip install --extra-index-url https://testpypi.python.org/pypi hausnet-server
# to avoid dependencies not being found.

import setuptools

# Get description from readme file.
with open("README.md", "r") as fh:
    long_description = fh.read()

# Get the frozen requirements
with open("requirements/base-frozen.pip") as fh:
    reqs = fh.read().splitlines()

setuptools.setup(
    name="hausnet_server",
    version="0.0.8",
    author="Louis Calitz",
    author_email="louis@hausnet.io",
    description="A server for the HausNet protocol",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/HausNet/hausnet-server",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=reqs
)
