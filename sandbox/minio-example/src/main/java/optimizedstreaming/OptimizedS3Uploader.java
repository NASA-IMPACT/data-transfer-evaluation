package optimizedstreaming;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OptimizedS3Uploader {

    private String bucketName = "testbucket";
    private String fileName = "hosts2";
    private String host = "192.168.0.18";
    private String accessKey = "admin";
    private String secretKey = "password";

    InitiateMultipartUploadResult initResponse;
    List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());
    AmazonS3 s3Client;

    public OptimizedS3Uploader(String bucketName, String fileName, String host, String accessKey, String secretKey) {
        this.bucketName = bucketName;
        this.fileName = fileName;
        this.host = host;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
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

    public void uploadChunk(int chunkId, String uploadFile) {
        File file = new File(uploadFile);
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(fileName)
                .withUploadId(initResponse.getUploadId())
                .withPartNumber(chunkId + 1)
                .withFileOffset(0)
                .withFile(file)
                .withPartSize(file.length());


        //System.out.println("Stream size " + is.getRemaining() + " for index " + index);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        this.partETags.add(uploadResult.getPartETag());
    }
}
