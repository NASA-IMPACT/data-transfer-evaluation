import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3DownloadExample {
    private static String bucketName = "gael-test";
    private static String keyName = "with_cloud/S2B_MSIL1C_20200704T154819_N0209_R054_T18SUJ_20200704T190951.zip";
    private static String host = "192.168.0.18";
    private static String accessKey = "7258d6c765464b06b9486e80924331a1";
    private static String secretKey = "c723e717356a4a21a71206fed1bbfff5";
    private static String downloadFile = "/tmp/download";

    public static void main(String args[]) {

        if (args.length == 1) {
            downloadFile = args[0];
        }

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://s3.gra.cloud.ovh.net/",
                        "GRA"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        try {
            //ObjectListing objectListing = s3Client.listObjects("gael-test");
            //System.out.println(objectListing.getNextMarker());
            // Download file
            long start = System.currentTimeMillis();
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, keyName);
            ObjectMetadata objectMetadata = s3Client.getObject(rangeObjectRequest, new File(downloadFile));
            System.out.println("Downloaded file " + (System.currentTimeMillis() - start) / 1000);

            //System.out.println("Printing bytes retrieved:");
            //displayTextInputStream(objectPortion.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
                    + "to Amazon S3, but was rejected with an error response" + " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());

        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " + "means the client encountered " + "an internal error while trying to "
                    + "communicate with S3, " + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());

        }

    }
}
