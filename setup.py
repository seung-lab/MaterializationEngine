from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="MaterializationEngine",
    version="0.1",
    author="Sven Dorkenwald",
    author_email="",
    description="Combines DynamicAnnotationDB and PyChunkedGraph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seung-lab/MaterializationEngine",
    packages=find_packages(),
    install_requires=[
        "dynamicannotationdb",
        "pychunkedgraph"
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ),
)
