A tool-agnostic data transfer evaluation framework.

# Installation

## System Requirements
* Linux or Mac OS
* Java 11 or higher
* Python 3.8 or higher

## Installing this evaluation tool

Use `pip` (preferred) or `setup.py` to install this package.

```bash
pip install -e .
```

or

```bash
python setup.py install
```

A better preferred way is to install this in a virtual environment.
- `python -m venv venv/` creates a virtual environment
- `venv/bin/activate` activates the environment
- `pip install -e .` installs the package in the virtual environment

## System setup
Please refer to [this SETUP guide](SETUP.md) which guides you through all the steps to setup tools needed like:
- setting up s3 emulation servers through minio
- setting up transfer tools like Rclone, MFT, Apache Nifi

# Configuration Setup
We use YAML-based configuration to feed configuration to our pipeline.
Edit the configuration file to update the S3 buckets details `data-transfer-evaluation/tests/config.yaml`.

## Transfer configuration YAML sample

```YAML
---
source_token: <username>
source_secret: <password>
source_s3_endpoint: "http://127.0.0.1:8080"
source_s3_bucket: "src"
source_s3_region: "us-east-1"

dest_token: <username>
dest_secret: <password>
dest_s3_endpoint: "http://127.0.0.1:8090"
dest_s3_bucket: "dest"
dest_s3_region: "us-east-1"
```

### For Source Server:
* `source_token` represents minio username
* `source_secret` represents minio password
* `source_s3_endpoint` is where we are running the minio source server
* `source_s3_bucket` is the name of the source directory
* `source_s3_region` emulates s3 region (just use defaults)

### For Destination Server:
* `dest_token` represents minio username
* `dest_secret` represents minio password
* `dest_s3_endpoint` is where we are running the minio destination server
* `dest_s3_bucket` is the name of the destination directory
* `dest_s3_region` emulates s3 region (just use defaults)

# Test Run Eval Framework

* Make sure minio servers and transfer tools are up and configured (see [setup guide](SETUP.md))
* Activate virtual environment if you have it set
* Create a transfer configuration YAML file (see the above "Configuration Setup" section for reference)
* Make sure you have s3 src/dest buckets (see [setup guide](SETUP.md) for details; or just run `scripts/buckets.sh` script)
* Make sure minio servers are up and running (see [setup guide](SETUP.md))
* Export below environment variables
    - `CFG_YAML`: Path to the transfer YAML config.yaml file
    - `NIFI_INSTALLATION`: Path to nifi base directory (eg: `/home/<username>/nifi/nifi-1.15.3/`)
    - `MFT_INSTALLATION` Path to mft base directory (eg: `/home/<username>/airawata-mft/`)
* Execute ```python tests/controller_test.py``` to initiate the transfer

> Note: You can add/modify the automation components params. See `tests/controller_test.py`.
