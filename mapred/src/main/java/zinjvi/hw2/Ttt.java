package zinjvi.hw2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Ttt {

    public static void main(String[] args) throws IOException {
        InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("test_resource.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        System.out.println("Line from resource = " + bufferedReader.readLine());
    }
}
