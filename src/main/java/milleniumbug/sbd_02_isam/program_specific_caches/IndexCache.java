package milleniumbug.sbd_02_isam.program_specific_caches;

import java.io.File;
import java.util.List;
import milleniumbug.sbd_02_isam.ISAM;

public class IndexCache extends ISAMCache<List<ISAM.IndexRecord>, IndexRecordSynchronizer> {

    public IndexCache(File f) {
        super(f, x -> new IndexRecordSynchronizer(x));
    }
    
    public IndexCache(File f, int size) {
        super(f, x -> new IndexRecordSynchronizer(x), size);
    }
}
