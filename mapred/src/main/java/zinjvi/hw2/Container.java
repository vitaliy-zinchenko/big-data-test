package zinjvi.hw2;

import java.io.IOException;

public class Container {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Container main");

        new SimpleHttpServer(8890).run();

//        Thread.sleep(100000L);

        //throw new RuntimeException("Bla!!!!!!!!!!!!!!!!!");
    }

}
