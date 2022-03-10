package streaming;

import java.io.*;

public class PartedInputStream extends InputStream {

    InputStream fileIs;
    int index;

    public PartedInputStream(String filePath, int index) throws FileNotFoundException {
        super();
        this.index = index;
        fileIs = new FileInputStream(new File(filePath + "/output_" + index + ".txt"));
    }

    @Override
    public long skip(long n) throws IOException {
        //System.out.println("Skipping " + index + ": " + n);
        return n;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        //System.out.println("Calling read2 index " + index + " : " + off + " " + len);
        return fileIs.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
        //System.out.println("Calling read1");
        return fileIs.read();
    }
}
