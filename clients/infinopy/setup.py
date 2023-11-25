from setuptools import setup, find_packages

setup(
    name="infinopy",
    version="0.0.6",
    author="Vinay K",
    author_email="vinaykakade@gmail.com",
    description="A Python package for Infino",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/infinohq/infino",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    install_requires=[
        "docker",
        "requests",
    ],
)
