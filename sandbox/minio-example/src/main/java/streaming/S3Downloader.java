package streaming;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class S3Downloader {


    private String bucketName = "testbucket";
    private String fileName = "hosts";
    private String host = "http://192.168.0.18:9000";
    private String accessKey = "admin";
    private String secretKey = "password";
    private long totalLen = 0;
    private long totalChunks = 0;
    private AmazonS3 s3Client;

    public S3Downloader(String bucketName, String fileName, String host, String accessKey, String secretKey) {
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

    public List<LimitInputStream> fetchParallelStreams(long chunkSize, long fileLength, long offset) {
        List<LimitInputStream> inputStreams = new ArrayList<>();

        while(offset < fileLength) {
            long nextStep = offset + chunkSize;
            if (nextStep > fileLength) {
                nextStep = fileLength;
            }

            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, fileName);

            rangeObjectRequest.setRange(offset, nextStep - 1);

            //System.out.println("Start length " + offset + " Ending length " + nextStep + " File length " + fileLength);
            S3Object object = s3Client.getObject(rangeObjectRequest);
            S3ObjectInputStream objectContent = object.getObjectContent();
            inputStreams.add(new LimitInputStream(objectContent, nextStep - offset));
            totalLen += nextStep - offset;
            totalChunks++;
            offset = nextStep;
        }
        System.out.println("Returned chunk " + totalChunks);

        //System.out.println("Total Download " + totalLen + " chunks " + totalChunks);
        return inputStreams;
    }


    public static void main(String args[]) throws ExecutionException, InterruptedException {

        String bucketName = "testbucket";
        String fileName = "hosts";
        String host = "http://192.168.0.18:9000";
        String accessKey = "admin";
        String secretKey = "password";
        String downloadDir = "/tmp";
        long chunkSize = 10 * 1024 * 1024L;

        if (args.length == 1) {
            downloadDir = args[0];
        }

        long startTime = System.currentTimeMillis();

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
            ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, fileName);
            long fileLength = objectMetadata.getContentLength();
            List<Future<Integer>> futures = new ArrayList<>();

            long startLength = 0L;
            int chunkIdx = 0;
            //System.out.println("File length " + fileLength);
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
                        new S3Downloader.RangedDownloadRunner(startLength, endingLength - 1, s3Client, chunkIdx,
                                downloadDir, bucketName, fileName));
                futures.add(resultFuture);
                chunkIdx++;
                startLength = endingLength;
            }

            //System.out.println("Total chunks " + chunkIdx);

            for (int i = 0; i < chunkIdx; i ++) {
                Integer res = futures.get(i).get();
                //System.out.println("Chunk " + res + " is completed");
            }

            long endTime = System.currentTimeMillis();
            //System.out.println("Transfer is done. Time " + (endTime - startTime) + " ms");

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
        private String downloadDir;
        private String bucketName;
        private String keyName;

        public RangedDownloadRunner(long start, long end, AmazonS3 s3Client, int chunkIdx, String downloadDir, String bucketName, String keyName) {
            this.start = start;
            this.end = end;
            this.s3Client = s3Client;
            this.chunkIdx = chunkIdx;
            this.downloadDir = downloadDir;
            this.bucketName = bucketName;
            this.keyName = keyName;
        }

        @Override
        public Integer call() {
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, keyName);
            rangeObjectRequest.setRange(start, end);
            ObjectMetadata objectMetadata = s3Client.getObject(rangeObjectRequest, new File(downloadDir + "/output_" + chunkIdx + ".txt"));
            //System.out.println("Downloaded chunk start " + start + " end " + end + " " + chunkIdx);
            return chunkIdx;
        }
    }
}
