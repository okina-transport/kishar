package org.entur.kishar.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TestUtils {

    private TestUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String readMockResponseFromFile(String filePath) {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return content.toString();
    }
}
