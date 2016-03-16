package zinjvi.hw2;

import com.sun.net.httpserver.HttpExchange;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

@FunctionalInterface
public interface Handler {

    String handle(HttpExchange httpExchange) throws IOException;

}
