/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_b_isam.program_specific_caches;

import milleniumbug.sbd_02_isam.program_specific_caches.SeqFileRecordSynchronizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import milleniumbug.sbd_02_isam.ISAM;
import milleniumbug.sbd_02_isam.generic_caches.MapSynchronizer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author milleniumbug
 */
public class SeqFileRecordSynchronizerTest {
    
    public SeqFileRecordSynchronizerTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
        @Test
    public void basic() {
        SeqFileRecordSynchronizer res = new SeqFileRecordSynchronizer(new MapSynchronizer<>(new HashMap<Long, byte[]>()));
        Consumer<List<ISAM.SeqFileRecord>> testCase = (a) -> {
            a = new ArrayList<>(a);
            byte[] serialize = res.serialize(0, a);
            List<ISAM.SeqFileRecord> b = res.deserialize(0, serialize);
            assertEquals(a, b);
        };
        testCase.accept(Arrays.asList());
        testCase.accept(Arrays.asList(new ISAM.SeqFileRecord(0, 2, "asdfgh", 0), new ISAM.SeqFileRecord(0, 3, "asdfgg", 0), new ISAM.SeqFileRecord(0, Long.MAX_VALUE, "asdfgg", Long.MAX_VALUE)));
    }
    
}
