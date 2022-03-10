package optimizedstreaming;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import streaming.S3Downloader;
import streaming.S3Uploader;

import java.io.File;
import java.util.concurrent.*;

public class OptimizedStreamer {

    private String sourceBucket = "testbucket";
    private String sourceFile = "hosts";
    private String sourceAccessKey = "admin";
    private String sourceSecretKey = "password";
    private String sourceHost = "http://192.168.0.18:9000";


    private String destBucket = "testbucket";
    private String destFile = "hosts2";
    private String destAccessKey = "admin";
    private String destSecretKey = "password";
    private String destHost = "http://192.168.0.18:9000";

    ExecutorService executorService = Executors.newFixedThreadPool(20);

    private CompletionService<Integer> completionService =
            new ExecutorCompletionService<Integer>(executorService);

    public void startStream() throws InterruptedException {

        long startTime = System.currentTimeMillis();

        long chunkSize = 20 * 1024 * 1024L;

        AWSCredentials credentials = new BasicAWSCredentials(sourceAccessKey, sourceSecretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sourceHost,
                        "GRA"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(sourceBucket, sourceFile);
        long fileLength = objectMetadata.getContentLength();

        OptimizedS3Downloader downloader = new OptimizedS3Downloader(sourceBucket, sourceFile, sourceHost, sourceAccessKey, sourceSecretKey);
        downloader.init();
        OptimizedS3Uploader uploader = new OptimizedS3Uploader(destBucket, destFile, destHost, destAccessKey, destSecretKey);
        uploader.init();

        long uploadLength = 0L;
        int chunkIdx = 0;

        while(uploadLength < fileLength) {

            long endPos = uploadLength + chunkSize;
            if (endPos > fileLength) {
                endPos = fileLength;
            }

            String tempFile = "/tmp/chunk" + chunkIdx;
            completionService.submit(new Mover(downloader, uploader, uploadLength, endPos, chunkIdx,tempFile));

            uploadLength = endPos;
            chunkIdx++;
        }


        for (int i = 0; i < chunkIdx; i++) {
            Future<Integer> future = completionService.take();
            //System.out.println("Chunk " + i + " was completed");
        }
        uploader.close();


        long endTime = System.currentTimeMillis();
        System.out.println("Transfer is done. Time " + (endTime - startTime) + " ms");
        executorService.shutdown();


    }

    private class Mover implements Callable<Integer> {

        OptimizedS3Downloader downloader;
        OptimizedS3Uploader uploader;
        long uploadLength;
        long endPos;
        int chunkIdx;
        String tempFile;

        public Mover(OptimizedS3Downloader downloader, OptimizedS3Uploader uploader, long uploadLength,
                     long endPos, int chunkIdx, String tempFile) {
            this.downloader = downloader;
            this.uploader = uploader;
            this.uploadLength = uploadLength;
            this.endPos = endPos;
            this.chunkIdx = chunkIdx;
            this.tempFile = tempFile;
        }

        @Override
        public Integer call() throws Exception {
            downloader.downloadChunk(uploadLength, endPos, tempFile);
            uploader.uploadChunk(chunkIdx, tempFile);
            new File(tempFile).delete();
            return chunkIdx;
        }
    }

    public static void main(String args[]) throws InterruptedException {
        OptimizedStreamer streamer = new OptimizedStreamer();
        streamer.startStream();
    }
}
