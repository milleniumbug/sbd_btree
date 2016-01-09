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

public class SeqFileRecordSynchronizer extends SerializerSynchronizer<List<ISAM.SeqFileRecord>> {

    public SeqFileRecordSynchronizer(CacheSynchronizer<Long, byte[]> synchronizer) {
        super(synchronizer);
    }

    @Override
    public List<ISAM.SeqFileRecord> deserialize(long pos, byte[] buffer) {
        try {
            List<ISAM.SeqFileRecord> res = new ArrayList<>(ISAM.SeqFileRecord.recordsPerPage);
            if(buffer == null)
                return res;
            DataInputStream reader = new DataInputStream(new ByteArrayInputStream(buffer));
            for(int i = 0; i < ISAM.SeqFileRecord.recordsPerPage; ++i) {
                boolean deleted = reader.readBoolean();
                long key = reader.readLong();
                long nextPointer = reader.readLong();
                byte[] v = new byte[30];
                reader.read(v);
                int chindex;
                for(chindex = 0; chindex < v.length; ++chindex)
                {
                    if(v[chindex] == 0)
                        break;
                }
                String value = new String(Arrays.copyOf(v, chindex));
                ISAM.SeqFileRecord rec = new ISAM.SeqFileRecord(pos, key, value, nextPointer);
                rec.deleted = deleted;
                if(rec.deleted && rec.key == Long.MAX_VALUE && rec.nextPointer == Long.MAX_VALUE)
                    break;
                res.add(rec);
            }
            Collections.sort(res);
            return res;
        } catch (IOException ex) {
            Logger.getLogger(ISAMCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] serialize(long pos, List<ISAM.SeqFileRecord> node) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream writer = new DataOutputStream(bos);
            ISAM.SeqFileRecord padding = new ISAM.SeqFileRecord(pos, Long.MAX_VALUE, "PUSTY", Long.MAX_VALUE);
            padding.deleted = true;
            node = new ArrayList<>(node);
            node.addAll(Collections.nCopies(
                    ISAM.SeqFileRecord.recordsPerPage - node.size(), 
                    padding));
            for(int i = 0; i < node.size(); ++i)
            {
                ISAM.SeqFileRecord rec = node.get(i);
                writer.writeBoolean(rec.deleted);
                writer.writeLong(rec.key);
                writer.writeLong(rec.nextPointer);
                byte[] bytes = rec.value.getBytes();
                bytes = Arrays.copyOf(bytes, 30);
                writer.write(bytes);
            }
            writer.flush();
            return Arrays.copyOf(bos.toByteArray(), FilePageSynchronizer.page_size);
        } catch (IOException ex) {
            Logger.getLogger(ISAMCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
