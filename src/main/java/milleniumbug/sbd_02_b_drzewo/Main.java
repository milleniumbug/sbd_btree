package milleniumbug.sbd_02_b_drzewo;

import java.io.File;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Supplier;

public class Main {

    public static void main(String[] args) throws Exception {
        String action = args[1];
        File filepath = new File(args[2]);

        Supplier<ISAM> load = () -> {
            switch (action) {
                case "C":
                    return ISAM.create(filepath);
                case "O":
                    return new ISAM(filepath);
                default:
                    return null;
            }
        };
        Scanner s = new Scanner(System.in);
        try (ISAM isam = load.get()) {
            while (true) {
                final String zapytanie
                        = "1. Znajdz dla klucza\n"
                        + "2. Dodaj wartość o kluczu\n"
                        + "3. Usun wartość o kluczu\n"
                        + "4. Zastąp wartość w miejscu\n"
                        + "5. Reorganizacja\n"
                        + "6. Wyświetl \"surową\" strukturę pliku\n"
                        + "7. Wyjdź z programu\n";
                System.out.println(zapytanie);
                final int choice = s.nextInt();
                if (choice == 1) {
                    System.out.println("Podaj klucz");
                    Optional<String> find = isam.find(s.nextLong());
                    System.out.println(find
                            .map(x -> "ZNALEZIONO: " + x)
                            .orElse("!!! NIE ZNALEZIONO DANEGO CIĄGU ZNAKÓW !!!"));
                }
                else if (choice == 2) {
                    System.out.println("Podaj klucz");
                    Long key = s.nextLong();
                    System.out.println("Podaj wartość");
                    String value = s.nextLine();
                    isam.insert(key, value);
                }
                else if (choice == 3) {
                    System.out.println("Podaj klucz");
                    Long key = s.nextLong();
                    isam.erase(key);
                }
                else if (choice == 4) {
                    System.out.println("Podaj klucz");
                    Long key = s.nextLong();
                    System.out.println("Podaj wartość");
                    String value = s.nextLine();
                    isam.insert(key, value);
                }
                else if (choice == 5) {
                    isam.reorganize();
                }
                else if (choice == 6) {
                    isam.printOutRawISAM();
                }
                else if (choice == 7) {
                    break;
                }
            }
        }
    }
}
