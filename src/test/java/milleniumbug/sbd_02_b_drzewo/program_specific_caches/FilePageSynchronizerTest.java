/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package milleniumbug.sbd_02_b_drzewo.program_specific_caches;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
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
public class FilePageSynchronizerTest {
    
    public FilePageSynchronizerTest() {
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
    public void basic() throws FileNotFoundException {
        FilePageSynchronizer filePageSynchronizer = new FilePageSynchronizer(new RandomAccessFile("asdf_index", "rw"));
        filePageSynchronizer.flush(0L, new byte[4096]);
        filePageSynchronizer.load(0L);
        assertArrayEquals(filePageSynchronizer.load(2L), null);
    }   
}
