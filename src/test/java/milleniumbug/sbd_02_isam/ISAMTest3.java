/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_isam;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import static milleniumbug.sbd_02_isam.ISAMTest2.runWithPrintReadWriteCounts;
import static milleniumbug.sbd_02_isam.ISAMTest2.testInsertBiggerData;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author milleniumbug
 */
public class ISAMTest3 {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    public static void forISAM(long size, String f, Consumer<ISAM> cnsmr) throws Exception {
        try (ISAM isam = new ISAM(new File(f))) {
            System.out.print(size);
            runWithPrintReadWriteCounts(isam, cnsmr);
        }
    }

    public void testRandomLookup(long size, String f) throws Exception {
        forISAM(size, f, i -> {
            List<Long> keys = LongStream.range(0, size).boxed().collect(Collectors.toCollection(ArrayList::new));
            Collections.shuffle(keys, new Random(0));
            for (Long key : keys.subList(0, 1000)) {
                i.insert(key, "asdf");
            }
        });
    }
    
    
    public void testSequentialLookup(long size, String f) throws Exception {
        forISAM(size, f, i -> {
            List<Long> keys = LongStream.range(0, 1000).boxed().collect(Collectors.toList());
            for (Long key : keys.subList(0, 1000)) {
                i.insert(key, "asdf");
            }
        });
    }
    
    @Test
    public void testSequentialLookup() throws Exception {
        testSequentialLookup(12500, "test1");
        testSequentialLookup(25000, "test2");
        testSequentialLookup(50000, "test3");
        testSequentialLookup(100000, "test4");
        testSequentialLookup(200000, "test5");
    }

    @Test
    public void testRandomLookup() throws Exception {
        testRandomLookup(12500, "test1");
        testRandomLookup(25000, "test2");
        testRandomLookup(50000, "test3");
        testRandomLookup(100000, "test4");
        testRandomLookup(200000, "test5");
    }

}
