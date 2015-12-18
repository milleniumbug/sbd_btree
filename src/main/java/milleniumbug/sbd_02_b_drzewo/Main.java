package milleniumbug.sbd_02_b_drzewo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;

public class Main {

    public static void main(@NonNull String[] args) {
        try (final RandomAccessFile r = new RandomAccessFile(new File("asdf.whatever"), "rw")) {
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
