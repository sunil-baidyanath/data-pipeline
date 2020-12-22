'''
Created on Dec 3, 2020

@author: sunil.thakur
'''
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="data-pipeline", 
    version="0.0.1",
    author="Sunil Thakur",
    author_email="sunil.thakur@gmail.com",
    description="A sample data pipeline package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sunil.baidyanath/data-pipeline",
    packages=['pipelines', 'pipelines.core', 'pipelines.sample'],
    package_dir={'data-pipeline':'pipelines'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    zip_safe = False
)
