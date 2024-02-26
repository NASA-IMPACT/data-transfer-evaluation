A tool-agnostic data transfer evaluation framework.


[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10709852.svg)](https://doi.org/10.5281/zenodo.10709852)


## Installation Steps

```bash
python setup.py install
```

or

```bash
pip install -e .
```

## System Requirements
* Linux or Mac OS
* Java 11 or higher
* Python 3.6 or higher

### Apache Nifi

* Download and unarchive Nifi binary from https://dlcdn.apache.org/nifi/1.15.3/nifi-1.15.3-bin.tar.gz
* Update property nifi.security.allow.anonymous.authentication=true in conf/nifi.properties
* Start Nifi server ```./bin/nifi.sh start --wait-for-init 120```
* After tests are completed, run ```./bin/nifi.sh start``` to stop the server

### Rclone

* Download and install rclone binary from ```curl https://rclone.org/install.sh | sudo bash```

### Airavata MFT

* Download and unarchive MFT Binary from https://github.com/apache/airavata-mft/releases/download/0.1-pre-release/airavata-mft-0.1.zip
* Start consul server by running ```./start_consul.sh mac``` or ```./start_consul.sh linux``` depending on the operating system
* Start Airavata MFT by running ```./start_mft.sh```
* After tests are completed, run ```./stop_mft.sh && ./stop_consul.sh``` to stop MFT services

### Examples and usages

See `tests/` directory for examples.

