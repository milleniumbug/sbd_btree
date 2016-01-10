/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_isam;

import milleniumbug.sbd_02_isam.ISAM;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
public class ISAMTest2 {

    public ISAMTest2() {
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

    public void testInsertBiggerData(long size, String f) throws Exception {
        new File(f+"_index").delete();
        new File(f+"_data").delete();
        try (ISAM isam = ISAM.create(new File(f))) {
            System.out.print(size);
            runWithPrintReadWriteCounts(isam, i -> {
                List<Long> keys = LongStream.range(0, size).boxed().collect(Collectors.toList());
                //Collections.shuffle(keys, new Random(0));
                for (Long key : keys) {
                    i.insert(key, "asdf");
                }
            });
        }
    }

    public void runWithPrintReadWriteCounts(ISAM isam, Consumer<ISAM> cnsmr) {
        final long readsBefore = isam.readCount();
        final long writesBefore = isam.writeCount();
        final long startTime = System.currentTimeMillis();
        cnsmr.accept(isam);
        final long endTime = System.currentTimeMillis();
        final long readsAfter = isam.readCount();
        final long writesAfter = isam.writeCount();
        System.out.println("(" + (readsAfter - readsBefore) + "+" + (writesAfter - writesBefore) + "), czas: " + (endTime - startTime));
    }

    @Test
    public void testInsertBiggerData() throws Exception {
        testInsertBiggerData(12500, "test1");
        testInsertBiggerData(25000, "test2");
        testInsertBiggerData(50000, "test3");
        testInsertBiggerData(100000, "test4");
        testInsertBiggerData(200000, "test5");
    }

}
