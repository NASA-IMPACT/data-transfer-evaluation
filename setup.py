from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

exec(open("evalit/__version__.py").read())

setup(
    name="evalit",
    version=__version__,
    description="An end-to-end data transfer evaluation framework. ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NASA-IMPACT/data-transfer-evaluation",
    author="NASA-IMPACT, Nish, Suresh, Patrick, Dmuthu",
    author_email="np0069@uah.edu",
    # license="MIT",
    python_requires=">=3.6",
    packages=["evalit", "evalit.rclone", "evalit.nifi", "evalit.mft"],
    install_requires=required,
    classifiers=[
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Data Transfer",
    ],
    zip_safe=False,
)
