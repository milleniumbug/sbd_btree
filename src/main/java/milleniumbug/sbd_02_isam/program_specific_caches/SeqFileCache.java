package milleniumbug.sbd_02_isam.program_specific_caches;

import java.io.File;
import java.util.List;
import milleniumbug.sbd_02_isam.ISAM;

public class SeqFileCache extends ISAMCache<List<ISAM.SeqFileRecord>, SeqFileRecordSynchronizer> {

    public SeqFileCache(File f) {
        super(f, x -> new SeqFileRecordSynchronizer(x));
    }
    
    public SeqFileCache(File f, int size) {
        super(f, x -> new SeqFileRecordSynchronizer(x), size);
    }
}
