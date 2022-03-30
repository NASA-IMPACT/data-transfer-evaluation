import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MinIOMultipartAdvanced {
    private static String bucketName = "testbucket";
    private static String keyName = "hosts";
    private static String host = "192.168.0.18";
    private static String accessKey = "admin";
    private static String secretKey = "password";
    private static String uploadFileName = "/Users/dimuthu/code/data-transfers/minio-example/data/1g.file";
    private static long chunkSize = 16;

    public static void main(String args[]) throws InterruptedException {

        if (args.length == 6) {
            host = args[0];
            accessKey = args[1];
            secretKey = args[2];
            uploadFileName = args[3];
            chunkSize = Long.parseLong(args[4]);
            keyName = args[5];
        }

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://" + host + ":9000",
                        Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        long partSize = chunkSize * 1024 * 1024;

        TransferManager tm = TransferManagerBuilder.standard()
                .withMultipartCopyPartSize(partSize)
                .withS3Client(s3Client)
                .build();

        long start = System.currentTimeMillis();
        File file = new File(uploadFileName);
        Upload upload = tm.upload(bucketName, keyName, file);

        upload.waitForCompletion();


        long end = System.currentTimeMillis();
        System.out.println("Completed the upload");
        System.out.println("Time " + (end - start)/1000 + "s");

        tm.shutdownNow();
    }
}
