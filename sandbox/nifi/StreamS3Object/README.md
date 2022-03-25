## StreamS3Object *Apache Nifi processor*

This processor is able to copy a list of objects from an S3 bucket to an other S3 bucket.
Source and destination can be two different cloud providers.

The input object list comes from a **ListS3** processor.

Disclaimer: the development is still in progress and for now it's only a proof of concept.

A template is available in /data-transfer-evaluation/code/nifi/template/StreamS3.xml.

#### Installation

- Compile the project:
  - `mvn clean install`
- Copy the created *nar* in your Nifi installation **lib/** directory :
  - `cp cp nifi-StreamS3Object-nar/target/nifi-StreamS3Object-nar-0.1.nar <nifi-install>/nifi-<version>/lib/`
- Restart Nifi
- Import the **StreamS3.xml** template

#### Processor properties

Some properties must be configured in order for the processor to work.

**Source configuration**
- **Source AWS Credentials Provider service** (mandatory): the controller used for authentication

OR
- **Source Access Key** (mandatory): access Key
- **Source Secret Key** (mandatory): secret key


- **Source** Endpoint Override (optional): custom endpoint if source is not on AWS
- **Bucket** (mandatory) : name of the source bucket

**Destination configuration**
- **AWS Credentials Provider** service (mandatory): the controller used for authentication

OR
- **Access Key** (mandatory): access Key
- **Secret Key** (mandatory): secret key


- **Region** (mandatory) : name of the destination region
- **Endpoint Override** (optional): custom endpoint if source is not on AWS
- **Bucket** (mandatory) : name of the source bucket
- **Storage Class** (mandatory, default=Standard): class of the objects
- **Multipart Threshold (MB)** (mandatory, default=5000): size in MB over which objects will be uploaded into multiple parts
- **Multipart Size (MB)** (mandatory, default=5000): size in MB of parts in case of multipart upload
- **Max connections** (mandatory, default=200): maximum concurrent connections that can be opened towards the destination endpoint
- **Max retries** (mandatory, default=3): maximum number of retries per copy before it is considered as failed
