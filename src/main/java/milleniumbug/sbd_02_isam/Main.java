package milleniumbug.sbd_02_isam;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static int log2nlz(long bits) {
        if (bits == 0) {
            return 0; // or throw exception
        }
        return 63 - Long.numberOfLeadingZeros(bits);
    }

    public static void wykonujPolecenia(ISAM isam, Scanner s, PrintStream out) throws FileNotFoundException {
        while (true) {
            final String zapytanie
                    = "1. Znajdz dla klucza\n"
                    + "2. Dodaj wartość o kluczu\n"
                    + "3. Usun wartość o kluczu\n"
                    + "4. Zastąp wartość w miejscu\n"
                    + "5. Reorganizacja\n"
                    + "6. Wyświetl \"surową\" strukturę pliku\n"
                    + "7. Wykonuj polecenia z pliku\n"
                    + "8. Wyjdź z programu\n";
            out.println(zapytanie);
            final long writes_before = isam.writeCount();
            final long reads_before = isam.readCount();
            final int choice = Integer.parseInt(s.nextLine().trim());
            if (choice == 1) {
                out.println("Podaj klucz");
                Optional<String> find = isam.find(Long.parseLong(s.nextLine().trim()));
                out.println(find
                        .map(x -> "ZNALEZIONO: " + x)
                        .orElse("!!! NIE ZNALEZIONO DANEGO CIĄGU ZNAKÓW !!!"));
            } else if (choice == 2) {
                out.println("Podaj klucz");
                Long key = Long.parseLong(s.nextLine().trim());
                out.println("Podaj wartość");
                String value = s.nextLine();
                isam.insert(key, value);
            } else if (choice == 3) {
                out.println("Podaj klucz");
                Long key = Long.parseLong(s.nextLine().trim());
                isam.erase(key);
            } else if (choice == 4) {
                out.println("Podaj klucz");
                Long key = Long.parseLong(s.nextLine().trim());
                out.println("Podaj wartość");
                String value = s.nextLine();
                isam.insert(key, value);
            } else if (choice == 5) {
                isam.reorganize();
            } else if (choice == 6) {
                isam.printOutRawISAM();
            } else if (choice == 7) {
                out.println("Podaj nazwę pliku");
                String path = s.nextLine();
                wykonujPolecenia(isam, new Scanner(new BufferedInputStream(new FileInputStream(new File(path)))), new PrintStream(new OutputStream() {
                    @Override
                    public void write(int i) throws IOException {

                    }
                }));
            } else if (choice == 8) {
                break;
            }
            final long writes_after = isam.writeCount();
            final long reads_after = isam.readCount();
            out.println("Odczytów: " + (reads_after - reads_before));
            out.println("Zapisów: " + (writes_after - writes_before));
        }
    }

    public static void main(String[] args) throws Exception {
        String action = args[0];
        File filepath = new File(args[1]);

        Supplier<ISAM> load = () -> {
            try {
                switch (action) {
                    case "C":
                        return ISAM.create(filepath);
                    case "O":
                        return new ISAM(filepath);
                    default:
                        return null;
                }
            } catch (Exception ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        };
        Scanner s = new Scanner(System.in);
        try (ISAM isam = load.get()) {
            wykonujPolecenia(isam, s, System.out);
        }
    }
}
