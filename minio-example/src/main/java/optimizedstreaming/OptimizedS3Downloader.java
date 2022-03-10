package optimizedstreaming;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.File;

public class OptimizedS3Downloader {

    private String bucketName = "testbucket";
    private String fileName = "hosts";
    private String host = "http://192.168.0.18:9000";
    private String accessKey = "admin";
    private String secretKey = "password";
    private long totalLen = 0;
    private long totalChunks = 0;
    private AmazonS3 s3Client;

    public OptimizedS3Downloader(String bucketName, String fileName, String host, String accessKey, String secretKey) {
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
                        "GRA"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }


    public void downloadChunk(long start, long end, String downloadPath) {
        GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, fileName);
        rangeObjectRequest.setRange(start, end - 1);
        ObjectMetadata objectMetadata = s3Client.getObject(rangeObjectRequest, new File(downloadPath));
    }


}
