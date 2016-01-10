/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_b_drzewo;

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

    public void testInsertBiggerData(long size) throws Exception {
        new File("test1_index").delete();
        new File("test1_data").delete();
        try (ISAM isam = ISAM.create(new File("test1"))) {
            System.out.print(size);
            runWithPrintReadWriteCounts(isam, i -> {
                List<Long> keys = LongStream.range(0, size).boxed().collect(Collectors.toList());
                Collections.shuffle(keys, new Random(0));
                for (Long key : keys) {
                    i.insert(key, "asdf");
                }
            });
        }
    }

    public void runWithPrintReadWriteCounts(ISAM isam, Consumer<ISAM> cnsmr) {
        final long readsBefore = isam.readCount();
        final long writesBefore = isam.writeCount();
        cnsmr.accept(isam);
        final long readsAfter = isam.readCount();
        final long writesAfter = isam.writeCount();
        System.out.print("(" + (readsAfter - readsBefore) + "+" + (writesAfter - writesBefore) + ")");
    }

    @Test
    public void testInsertBiggerData() throws Exception {
        testInsertBiggerData(200000);
        testInsertBiggerData(400000);
        testInsertBiggerData(800000);
        testInsertBiggerData(1600000);
        testInsertBiggerData(3200000);
    }

}
