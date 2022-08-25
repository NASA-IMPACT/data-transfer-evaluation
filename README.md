A tool-agnostic data transfer evaluation framework.

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
* Python 3.9 or higher


### Setup local S3 server using minio

* Download minio server binary from https://min.io/download
* Make sure to add minio binary to any of the system paths (/home/<user>/bin/, /usr/local/bin/ or /bin/)
* Create a local folder/directory that will be emulated as the buckets
* Create a temporary directory say ```mkdir /tmp/transfer-eval/```
* Create 2 directories inside it: ```mkdir /tmp/transfer-eval/src``` & ```mkdir /tmp/transfer-eval/dest```


### Setup Source Buckets
* Open the first terminal (say A)
* Go to the emulate bucket directory (say /tmp/transfer-eval)
* Set the username and password in this command that we will use for the authentication while transferring + run the source server (MINIO_ROOT_USER=<username> MINIO_ROOT_PASSWORD=<password> minio server ./<path> --address :<port>)
* This will run the source bucket server at port <port> Eg:  ```MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password minio server ./src --address :8080```


### Setup Destination Buckets
* Open the first terminal (say B)
* Go to the emulate bucket directory (say /tmp/transfer-eval)
* Set the username and password in this command that we will use for the authentication while transferring + run the source server (MINIO_ROOT_USER=<username> MINIO_ROOT_PASSWORD=<password> minio server ./<path> --address :<port>)
* This will run the destination bucket server at port <port> Eg:  ```MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password minio server ./dest --address :8080```

### Emulate dummy source files

* Use any UNIX tool to create a dummy file (it can be anything).
* For brevity, we use `dd` command ```dd if=/dev/zero of=/tmp/transfer-eval/src/testfile.1 bs=1024 count=1000000 This creates a dummy file ~1GB```

### Configuration

* checkout the source code ```git clone https://github.com/NASA-IMPACT/data-transfer-evaluation.git```
* Edit the configuration file to update the S3 buckets details ```vi data-transfer-evaluation/tests/config.yaml```

### Rclone Installation

* Download and install rclone binary from ```curl https://rclone.org/install.sh | sudo bash```

### Apache Nifi

* Download and unarchive Nifi binary from https://dlcdn.apache.org/nifi/1.15.3/nifi-1.15.3-bin.tar.gz
* Unarchive the tar file ```tar -xf nifi-1.15.3-bin.tar.gz```
* Update property nifi.security.allow.anonymous.authentication=true in nifi-1.15.3/conf/nifi.properties
* Start Nifi server ```./bin/nifi.sh start --wait-for-init 120```
* After tests are completed, run ```./bin/nifi.sh stop``` to stop the server

### Airavata MFT

* Download and unarchive MFT Binary from https://github.com/apache/airavata-mft/releases/download/0.1-pre-release/airavata-mft-0.1.zip
* Unzip the zip file ```unzip airavata-mft.zip```
* Start consul server by running ```./start-consul.sh mac``` or ```./start-consul.sh linux``` depending on the operating system
* Start Airavata MFT by running ```./start-mft.sh```
* After tests are completed, run ```./stop-mft.sh && ./stop-consul.sh``` to stop MFT services

### Examples and usages

* cd into `tests/` directory for examples.
* Execute ```python3 controller_test2.py``` to initiate the transfer

