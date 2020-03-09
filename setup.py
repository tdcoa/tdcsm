import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tdcsm",
    version="0.2.4",
    author="Stephen Hilton",
    author_email="Stephen@FamilyHilton.com",
    description="Teradata tools for CSMs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tdcoa/tdcsm",
    packages=setuptools.find_packages(),
    install_requires=[
          'pandas',
          'teradatasqlalchemy',
          'teradataml'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
