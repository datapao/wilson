import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", 'r') as reqfile:
    requirements = reqfile.readlines()

setuptools.setup(
    name="nelson",
    version="0.0.1",
    author="datapao",
    author_email="andras.fulop@datapao.com",
    description="Six sigma rule generator on Spark DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url="https://github.com/datapao/nelson/archive/v0.0.1.zip",
    url="https://github.com/datapao/nelson",
    packages=setuptools.find_packages(),
    license='Apache License 2.0',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=requirements
)