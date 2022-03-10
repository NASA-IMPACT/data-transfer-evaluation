### Known Facts:

Nifi uses standard S3 SDK libraries to upload and download. Upload is done through Multipart uploads which makes the
upload [1] multi threaded but download is single threaded [2]. It seems like Nifi uses an intermediate temp file [3] to map
s3 senders and receivers (I guess). This is expected as Nifi needs to touch the content of the file if transformations are
required. In Airavata MFT, senders are receivers are connected through a streaming buffer which do not involve an
intermediate file as we are not interested in touching the content of the file.

#### Possible implementation bug in Nifi multipart upload

Even though Nifi do multipart uploads, they use the low level API example code provided in [6]. Look at [5]. This loop needs to be
executed parallely to gain the advantage of multipart uploads or it will be as same as a single thread upload. I tested a sample 
code and verified. The gain of running that piece parallely is 5x on a 5Gbps and 2s RTT network. I guess this is the reason why
Patrik ran parallel file uploads to saturate network.

### Improvements

Airavata MFT S3 implementation is also single threaded so I beleive that we do not see major improvements compared to Nifi
for small files. But we might see some improvements for large files as there we are not writing an intermediate file to
the local disk.

Next major improvement that we can consider is multi threaded transfer of a S3 file (Transferring portions of a large
file in parallel threads). S3 SDK directly does not provide this but there are ways to do this at the low level S3 APIs and
we have to handle the concurrency and failover. This approach is conceptually similar to what GridFTP does for TCP with
parallel TCP streams. The advantage comes into play when the data link is too long (inter-continal) and prone to have more
missing packets. In such a case, breakdown of a single TCP stream does not affect the transfer of the entire file.

I plan to write an example code which can do stripped S3 transfers between two s3 endpoints (AWS S3 and ESA Openstack).
Initial simulations are planned to do using MinIO S3 compatible APIs and Emulab infrastructure.


[1] https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3/PutS3Object.java#L693

[2] https://github.com/apache/nifi/blob/ad5b816626f6721b787ea4142831042f82cdb7cc/nifi-nar-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3/FetchS3Object.java#L210

[3] https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3/PutS3Object.java#L525

[4] https://min.io/download

[5] https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3/PutS3Object.java#L741

[6] https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html
