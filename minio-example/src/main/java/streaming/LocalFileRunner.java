package streaming;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

public class LocalFileRunner {

    public static void main(String args[]) throws ExecutionException, InterruptedException, FileNotFoundException {
        long startTime = System.currentTimeMillis();
        S3Downloader.main(args);
        S3Uploader.main(args);
        long endTime = System.currentTimeMillis();
        System.out.println("Total Transfer is done. Time " + (endTime - startTime) + " ms");
    }
}
