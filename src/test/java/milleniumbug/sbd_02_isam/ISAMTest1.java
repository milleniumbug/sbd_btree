/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_isam;

import milleniumbug.sbd_02_isam.ISAM;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
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
public class ISAMTest1 {

    public ISAMTest1() {
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

    public void testFindData1(ISAM isam) {
        assertEquals(Optional.of("b"), isam.find(1));
        assertEquals(Optional.of("c"), isam.find(5));
        assertEquals(Optional.of("d"), isam.find(7));
        assertEquals(Optional.of("e"), isam.find(8));
        assertEquals(Optional.of("f"), isam.find(10));
        assertEquals(Optional.of("g"), isam.find(11));
        assertEquals(Optional.of("h"), isam.find(12));
        assertEquals(Optional.of("i"), isam.find(13));
        assertEquals(Optional.of("j"), isam.find(21));

    }

    @Test
    public void testFindData1Basic() {
        ISAM isam = ISAM.testData1();
        testFindData1(isam);
        assertEquals(Optional.empty(), isam.find(0));
        assertEquals(Optional.empty(), isam.find(4));
        assertEquals(Optional.empty(), isam.find(6));
        assertEquals(Optional.empty(), isam.find(9));
        assertEquals(Optional.empty(), isam.find(14));
        assertEquals(Optional.empty(), isam.find(22));
    }

    @Test
    public void testDeleteOneData1() {
        BiConsumer<Long, Optional<String>> test_erase_cycle = (key, value) -> {
            ISAM isam = ISAM.testData1();
            assertEquals(value, isam.find(key));
            isam.erase(key);
            assertEquals(Optional.empty(), isam.find(key));
        };
        test_erase_cycle.accept(0L, Optional.empty());
        test_erase_cycle.accept(1L, Optional.of("b"));
        test_erase_cycle.accept(4L, Optional.empty());
        test_erase_cycle.accept(5L, Optional.of("c"));
        test_erase_cycle.accept(6L, Optional.empty());
        test_erase_cycle.accept(7L, Optional.of("d"));
        test_erase_cycle.accept(8L, Optional.of("e"));
        test_erase_cycle.accept(9L, Optional.empty());
        test_erase_cycle.accept(10L, Optional.of("f"));
        test_erase_cycle.accept(11L, Optional.of("g"));
        test_erase_cycle.accept(12L, Optional.of("h"));
        test_erase_cycle.accept(13L, Optional.of("i"));
        test_erase_cycle.accept(14L, Optional.empty());
        test_erase_cycle.accept(21L, Optional.of("j"));
        test_erase_cycle.accept(22L, Optional.empty());
    }
    
    @Test
    public void testReorganize()
    {
        ISAM isam = ISAM.testData1();
        testFindData1(isam);
        isam.printOutRawISAM();
        isam.reorganize();
        isam.printOutRawISAM();
        testFindData1(isam);
    }

    @Test
    public void testDeleteAllData1() {
        ISAM isam = ISAM.testData1();
        BiConsumer<Long, Optional<String>> test_erase_cycle = (key, value) -> {
            assertEquals(value, isam.find(key));
            isam.erase(key);
            assertEquals(Optional.empty(), isam.find(key));
        };
        test_erase_cycle.accept(0L, Optional.empty());
        test_erase_cycle.accept(1L, Optional.of("b"));
        test_erase_cycle.accept(4L, Optional.empty());
        test_erase_cycle.accept(5L, Optional.of("c"));
        test_erase_cycle.accept(6L, Optional.empty());
        test_erase_cycle.accept(7L, Optional.of("d"));
        test_erase_cycle.accept(8L, Optional.of("e"));
        test_erase_cycle.accept(9L, Optional.empty());
        test_erase_cycle.accept(10L, Optional.of("f"));
        test_erase_cycle.accept(11L, Optional.of("g"));
        test_erase_cycle.accept(12L, Optional.of("h"));
        test_erase_cycle.accept(13L, Optional.of("i"));
        test_erase_cycle.accept(14L, Optional.empty());
        test_erase_cycle.accept(21L, Optional.of("j"));
        test_erase_cycle.accept(22L, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveSentinel() {
        ISAM isam = ISAM.testData1();
        isam.erase(Long.MIN_VALUE);
    }

    @Test
    public void testInsert() {
        ISAM isam = ISAM.testData1();
        isam.insert(3, "z");
        isam.printOutRawISAM();
        System.out.println("NEXT");
        testFindData1(isam);
        assertEquals(Optional.of("z"), isam.find(3));
        for (int key : IntStream.range(22, 28).toArray()) {
            isam.insert(key, "z");
            isam.printOutRawISAM();
            System.out.println("NEXT");
            testFindData1(isam);
            for (int test : IntStream.range(22, key).toArray()) {
                assertEquals(Optional.of("z"), isam.find(test));
            }
        }
    }
    
    @Test
    public void testInplaceReplace() {
        ISAM isam = ISAM.testData1();
        assertEquals(Optional.of("d"), isam.find(7));
        isam.insert(7, "asdf");
        assertEquals(Optional.of("asdf"), isam.find(7));
        assertEquals(Optional.of("j"), isam.find(21));
        isam.insert(21, "lolz");
        assertEquals(Optional.of("lolz"), isam.find(21));
    }
}
