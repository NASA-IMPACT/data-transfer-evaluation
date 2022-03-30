package streaming;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class S3Uploader {

    private String bucketName = "testbucket";
    private String fileName = "hosts2";
    private String host = "192.168.0.18";
    private String accessKey = "admin";
    private String secretKey = "password";
    private AtomicLong totalUpload = new AtomicLong();

    InitiateMultipartUploadResult initResponse;
    List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());
    AmazonS3 s3Client;

    public S3Uploader(String bucketName, String fileName, String host, String accessKey, String secretKey) {
        this.bucketName = bucketName;
        this.fileName = fileName;
        this.host = host;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        totalUpload.set(0L);
    }

    public void init() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(host,
                        Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, fileName);
        initResponse = s3Client.initiateMultipartUpload(initRequest);
    }

    public void close() {
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, fileName,
                initResponse.getUploadId(), partETags);
        s3Client.completeMultipartUpload(compRequest);
        //System.out.println("Total upload " + totalUpload.get());
    }

    public void pushParallelStream(LimitInputStream is, int index) {
        //System.out.println("Uploading " + index + " of size " + is.getRemaining());
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(fileName)
                .withUploadId(initResponse.getUploadId())
                .withPartNumber(index)
                .withFileOffset(0)
                .withInputStream(is)
                .withPartSize(is.getRemaining());

        totalUpload.set(totalUpload.get() + is.getRemaining());

        //System.out.println("Stream size " + is.getRemaining() + " for index " + index);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        this.partETags.add(uploadResult.getPartETag());
    }


    public static void main(String args[]) throws FileNotFoundException, ExecutionException, InterruptedException {

        String bucketName = "testbucket";
        String fileName = "hosts2";
        String host = "http://192.168.0.18:9000";
        String accessKey = "admin";
        String secretKey = "password";
        String dataDir = "/tmp";

        long contentLength = 2097152000;
        long partSize = 10 * 1024 * 1024L;

        long startTime = System.currentTimeMillis();

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        ExecutorService executorService = Executors.newFixedThreadPool(20);

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration( host ,
                        Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        List<PartETag> partETags = new ArrayList<>();

        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, fileName);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);


        // Upload the file parts.
        long filePosition = 0;
        List<Future<PartETag>> etagFutures = new ArrayList<>();

        for (int i = 1; filePosition < contentLength; i++) {
            // Because the last part could be less than 5 MB, adjust the part size as needed.
            partSize = Math.min(partSize, (contentLength - filePosition));

            Future<PartETag> partETagFuture = executorService.submit(new MultipartUploadRunner(i, partSize, 0, initResponse,
                    s3Client, dataDir, bucketName, fileName));
            etagFutures.add(partETagFuture);
            filePosition += partSize;
        }

        for (Future<PartETag> etagFuture : etagFutures) {
            PartETag partETag = etagFuture.get();
            partETags.add(partETag);
        }

        executorService.shutdown();
        // Complete the multipart upload.
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, fileName,
                initResponse.getUploadId(), partETags);
        s3Client.completeMultipartUpload(compRequest);

        long endTime = System.currentTimeMillis();
        System.out.println("Transfer is done. Time " + (endTime - startTime) + " ms");

    }

    private static class MultipartUploadRunner implements Callable<PartETag> {

        private int index;
        private long partSize;
        private long filePosition;
        private InitiateMultipartUploadResult initResponse;
        private AmazonS3 s3Client;
        private String dataDir;
        private String bucketName;
        private String keyName;
        public MultipartUploadRunner(int index, long partSize, long filePosition, InitiateMultipartUploadResult initResponse, AmazonS3 s3Client,
                                     String dataDir, String bucketName, String keyName) {
            this.index = index;
            this.partSize = partSize;
            this.filePosition = filePosition;
            this.initResponse = initResponse;
            this.s3Client = s3Client;
            this.dataDir = dataDir;
            this.bucketName = bucketName;
            this.keyName = keyName;
        }
        @Override
        public PartETag call() throws Exception {

            InputStream fis = new PartedInputStream(dataDir, index - 1);
            // Create the request to upload a part.
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(keyName)
                    .withUploadId(initResponse.getUploadId())
                    .withPartNumber(index)
                    .withFileOffset(filePosition)
                    .withInputStream(fis)
                    .withPartSize(partSize);

            // Upload the part and add the response's ETag to our list.
            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);

            return uploadResult.getPartETag();
        }
    }


}
