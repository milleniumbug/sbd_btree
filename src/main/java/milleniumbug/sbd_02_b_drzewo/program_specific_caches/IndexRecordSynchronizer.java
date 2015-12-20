package milleniumbug.sbd_02_b_drzewo.program_specific_caches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import milleniumbug.sbd_02_b_drzewo.ISAM;
import milleniumbug.sbd_02_b_drzewo.generic_caches.CacheSynchronizer;

public class IndexRecordSynchronizer extends SerializerSynchronizer<List<ISAM.IndexRecord>> {

    public IndexRecordSynchronizer(CacheSynchronizer<Long, byte[]> synchronizer) {
        super(synchronizer);
    }

    @Override
    public List<ISAM.IndexRecord> deserialize(long pos, byte[] buffer) {
        try {
            DataInputStream reader = new DataInputStream(new ByteArrayInputStream(buffer));
            List<ISAM.IndexRecord> res = new ArrayList<>(ISAM.IndexRecord.recordsPerPage);
            for (int i = 0; i < ISAM.IndexRecord.recordsPerPage; ++i) {
                long key = reader.readLong();
                long target = reader.readLong();
                if (key == Long.MAX_VALUE) {
                    break;
                }
                res.add(new ISAM.IndexRecord(pos, key, target));
            }
            Collections.sort(res);
            return res;
        } catch (IOException ex) {
            Logger.getLogger(ISAMCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] serialize(long pos, List<ISAM.IndexRecord> node) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream writer = new DataOutputStream(bos);
            ISAM.IndexRecord padding = new ISAM.IndexRecord(pos, Long.MAX_VALUE, Long.MAX_VALUE);
            node = new ArrayList<>(node);
            node.addAll(Collections.nCopies(
                    ISAM.IndexRecord.recordsPerPage - node.size(),
                    padding));
            for (int i = 0; i < node.size(); ++i) {
                ISAM.IndexRecord rec = node.get(i);
                writer.writeLong(rec.key);
                writer.writeLong(rec.page);
            }
            writer.flush();
            return Arrays.copyOf(bos.toByteArray(), FilePageSynchronizer.page_size);
        } catch (IOException ex) {
            Logger.getLogger(ISAMCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
