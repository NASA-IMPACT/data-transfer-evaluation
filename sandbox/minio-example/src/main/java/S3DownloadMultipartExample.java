import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.FileLocks;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class S3DownloadMultipartExample {

    private static String bucketName = "testbucket";
    private static String keyName = "hosts";
    private static String host = "http://192.168.0.18:9000";
    private static String accessKey = "admin";
    private static String secretKey = "password";
    private static String downloadDir = "/tmp/downloaddata";

    public static void main(String args[]) throws ExecutionException, InterruptedException {

        if (args.length == 1) {
            downloadDir = args[0];
        }

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(host,
                        "GRA"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        try {
            //ObjectListing objectListing = s3Client.listObjects("gael-test");
            //System.out.println(objectListing.getNextMarker());
            // Download file
            ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, keyName);
            long fileLength = objectMetadata.getContentLength();
            long chunkSize = 20 * 1024 * 1024L;
            long start = System.currentTimeMillis();
            List<Future<Integer>> futures = new ArrayList<>();

            long startLength = 0L;
            int chunkIdx = 0;
            while(fileLength > 0) {
                long endingLength = startLength;
                if (fileLength > chunkSize) {
                    endingLength += chunkSize;
                    fileLength -= chunkSize;
                } else {
                    endingLength += fileLength;
                    fileLength = 0;
                }
                Future<Integer> resultFuture = executorService.submit(
                        new S3DownloadMultipartExample.RangedDownloadRunner(startLength, endingLength, s3Client, chunkIdx));
                futures.add(resultFuture);
                chunkIdx++;
            }

            System.out.println("Total chunks " + chunkIdx);

            for (int i = 0; i < chunkIdx; i ++) {
                Integer res = futures.get(i).get();
                System.out.println("Chunk " + res + " is completed");
            }
            System.out.println("Downloaded file " + (System.currentTimeMillis() - start) / 1000);

            executorService.shutdown();
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

    public static class RangedDownloadRunner implements Callable<Integer> {

        private long start;
        private long end;
        private AmazonS3 s3Client;
        private int chunkIdx;

        public RangedDownloadRunner(long start, long end, AmazonS3 s3Client, int chunkIdx) {
            this.start = start;
            this.end = end;
            this.s3Client = s3Client;
            this.chunkIdx = chunkIdx;
        }

        @Override
        public Integer call() {
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, keyName);
            rangeObjectRequest.setRange(start, end);
            ObjectMetadata objectMetadata = s3Client.getObject(rangeObjectRequest, new File(downloadDir + "/output_" + chunkIdx + ".txt"));
            System.out.println("Downloaded chunk " + chunkIdx);
            return chunkIdx;
        }
    }
}
