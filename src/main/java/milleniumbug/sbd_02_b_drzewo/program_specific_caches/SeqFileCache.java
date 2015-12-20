package milleniumbug.sbd_02_b_drzewo.program_specific_caches;

import java.io.File;
import java.util.List;
import milleniumbug.sbd_02_b_drzewo.ISAM;

public class SeqFileCache extends ISAMCache<List<ISAM.SeqFileRecord>, SeqFileRecordSynchronizer> {

    public SeqFileCache(File f) {
        super(f, x -> new SeqFileRecordSynchronizer(x));
    }
}
