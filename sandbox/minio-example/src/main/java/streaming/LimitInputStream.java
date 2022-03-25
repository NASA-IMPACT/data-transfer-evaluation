package streaming;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.util.concurrent.atomic.AtomicBoolean;

public class LimitInputStream extends FilterInputStream implements Channel {
    private final AtomicBoolean open = new AtomicBoolean(true);
    private long remaining;

    public LimitInputStream(InputStream in, long length) {
        super(in);
        this.remaining = length;
    }

    public boolean isOpen() {
        return this.open.get();
    }

    public int read() throws IOException {
        if (!this.isOpen()) {
            throw new IOException("read() - stream is closed (remaining=" + this.remaining + ")");
        } else if (this.remaining > 0L) {
            --this.remaining;
            return super.read();
        } else {
            return -1;
        }
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (!this.isOpen()) {
            throw new IOException("read(len=" + len + ") stream is closed (remaining=" + this.remaining + ")");
        } else {
            int nb = len;
            if ((long)len > this.remaining) {
                nb = (int)this.remaining;
            }

            if (nb > 0) {
                int read = super.read(b, off, nb);
                this.remaining -= (long)read;
                return read;
            } else {
                return -1;
            }
        }
    }

    public long skip(long n) throws IOException {
        if (!this.isOpen()) {
            throw new IOException("skip(" + n + ") stream is closed (remaining=" + this.remaining + ")");
        } else {
            long skipped = super.skip(n);
            this.remaining -= skipped;
            return skipped;
        }
    }

    public int available() throws IOException {
        if (!this.isOpen()) {
            throw new IOException("available() stream is closed (remaining=" + this.remaining + ")");
        } else {
            int av = super.available();
            return (long)av > this.remaining ? (int)this.remaining : av;
        }
    }

    public void close() throws IOException {
        if (!this.open.getAndSet(false)) {
            ;
        }
    }

    public long getRemaining() {
        return remaining;
    }
}
