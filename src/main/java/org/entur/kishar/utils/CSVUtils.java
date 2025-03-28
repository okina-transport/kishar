package org.entur.kishar.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CSVUtils {
    
    /**
     * Read a csv file and builds a collection of records
     *
     * @param file the file to read
     * @return a collection of records
     * @throws IOException
     */
    public static List<CSVRecord> getRecords(File file) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream targetStream = new FileInputStream(file)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = targetStream.read(buffer)) > -1) {
                baos.write(buffer, 0, len);
            }
        }
        baos.flush();
        InputStream is1 = new ByteArrayInputStream(baos.toByteArray());
        InputStream is2 = new ByteArrayInputStream(baos.toByteArray());
        String result = IOUtils.toString(is1, StandardCharsets.UTF_8);

        String delimiter = guessDelimiter(result);

        Reader reader = new InputStreamReader(is2);

        return CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(false)
                .setDelimiter(delimiter)
                .get()
                .parse(reader)
                .getRecords();
    }

    private static String guessDelimiter(String fileContent) {
        String[] lines = fileContent.split("\n");
        String firstLine = lines[0];
        long nbOfSemiColon = firstLine.chars()
                .filter(ch -> ch == ';')
                .count();

        long nbOfComma = firstLine.chars()
                .filter(ch -> ch == ',')
                .count();

        return nbOfSemiColon > nbOfComma ? ";" : ",";
    }
}
