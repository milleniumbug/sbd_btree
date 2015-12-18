package milleniumbug.sbd_02_b_drzewo.program_specific_caches;

import milleniumbug.sbd_02_b_drzewo.generic_caches.CacheSynchronizer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;

public class FilePageSynchronizer implements CacheSynchronizer<Long, byte[]>, AutoCloseable {

    @NonNull
    private final RandomAccessFile file;

    public static final int page_size = 4096;

    public FilePageSynchronizer(@NonNull RandomAccessFile file) {
        this.file = file;
    }

    private long getPageNumber(long pos) {
        return pos / page_size * page_size;
    }

    @Override
    public byte[] load(@NonNull Long pos) {
        try {
            file.seek(getPageNumber(pos));
            byte[] buffer = new byte[page_size];
            int read = file.read(buffer);
            assert read == -1 || read == page_size;
            return buffer;
        } catch (IOException ex) {
            Logger.getLogger(FilePageSynchronizer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void flush(@NonNull Long key, @NonNull byte[] value) {
        try {
            assert value.length == 4096;
            file.seek(getPageNumber(key));
            file.write(value);
        } catch (IOException ex) {
            Logger.getLogger(FilePageSynchronizer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws Exception {
        file.close();
    }
}
