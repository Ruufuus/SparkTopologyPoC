package org.example;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class FileReader {

    public String readFileData(String path) throws IOException {
        File file = new File(path);
        InputStream inputStream = new FileInputStream(file);
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).toString();
    }
}
