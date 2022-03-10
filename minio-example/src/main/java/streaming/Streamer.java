package streaming;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class Streamer {

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

    private ExecutorService executorService = Executors.newFixedThreadPool(20);
    private CompletionService<Long> completionService =
            new ExecutorCompletionService<Long>(executorService);
    private class StreamProcessor implements Callable<Long> {

        final private LimitInputStream is;
        final private S3Uploader uploader;
        final private int index;

        public StreamProcessor(LimitInputStream is, S3Uploader uploader, int index) {
            this.is = is;
            this.uploader = uploader;
            this.index = index;
        }

        @Override
        public Long call() throws Exception {
            uploader.pushParallelStream(is, index + 1);
            return is.getRemaining();
        }
    }

    private void startStreaming() throws ExecutionException, InterruptedException {

        long startTime = System.currentTimeMillis();

        long chunkSize = 10 * 1024 * 1024L;
        int parallelStreams = 8;

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

        S3Downloader downloader = new S3Downloader(sourceBucket, sourceFile, sourceHost, sourceAccessKey, sourceSecretKey);
        downloader.init();
        S3Uploader uploader = new S3Uploader(destBucket, destFile, destHost, destAccessKey, destSecretKey);
        uploader.init();

        long offset = 0L;
        int chunkIndex = 0;

        long endLength = offset + chunkSize * parallelStreams;
        if (fileLength < endLength) {
            endLength = fileLength;
        }


        //System.out.println("Start length : " + offset + ", End length : " + endLength);
        List<LimitInputStream> inputStreams = downloader.fetchParallelStreams(chunkSize, endLength, offset);

        do  {

            List<Future<Long>> uploadFutures = new ArrayList<>();

            for (int i = 0; i < inputStreams.size(); i++) {
                //System.out.println("Chunk idx " + chunkIndex);
                Future<Long> future = completionService.submit(new StreamProcessor(inputStreams.get(i), uploader, chunkIndex));
                uploadFutures.add(future);
                chunkIndex++;
            }

            offset = endLength;

            for (int i = 0; i < uploadFutures.size(); i++) {
                Future<Long> future = completionService.take();
                Long remaining = future.get();

                if (offset < fileLength) {
                    endLength = endLength + chunkSize;
                    if (fileLength < endLength) {
                        endLength = fileLength;
                    }
                    List<LimitInputStream> limitInputStreams = downloader.fetchParallelStreams(chunkSize, endLength, offset);
                    Future<Long> submitFuture = completionService.submit(new StreamProcessor(limitInputStreams.get(0), uploader, chunkIndex));
                    uploadFutures.add(submitFuture);
                    chunkIndex++;
                    offset = endLength;
                    //System.out.println("Remaining for chunk " + i + " : " + remaining);
                }
            }


            offset = endLength;

        } while (offset < fileLength);

        System.out.println("Total chinks " + chunkIndex);
        executorService.shutdown();
        uploader.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Transfer is done. Time " + (endTime - startTime) + " ms");

    }

    public static void main(String a[]) throws ExecutionException, InterruptedException {
        Streamer s = new Streamer();
        s.startStreaming();
    }
}
