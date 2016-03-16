package zinjvi.hw2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SimpleHttpServer {

    private boolean stopped;

    private Map<String, Handler> handlers;

    private int port = 8889;

    public SimpleHttpServer(int port) {
        this.port = port;
        this.handlers = new HashMap<String, Handler>();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new SimpleHttpServer(8889).run();
    }

    public void addHandler(String command, Handler handler) {
        handlers.put(command, handler);
    }

    public void run() throws IOException, InterruptedException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(port);

        final HttpServer httpServer = HttpServer.create(inetSocketAddress, 0);
        httpServer.createContext("/am", new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                URI uri = httpExchange.getRequestURI();
                String command = uri.getQuery();
                if ("stop".equals(command)) {
                    System.out.println("Stop request");
                    Handler stopHandler = handlers.get(command);
                    if (stopHandler != null) {
                        stopHandler.handle(httpExchange);
                    }
                    String resp = "Stopping...";
                    httpExchange.sendResponseHeaders(200, resp.length());
                    OutputStream responseBody = httpExchange.getResponseBody();
                    responseBody.write(resp.getBytes());
                    responseBody.close();
                    httpServer.stop(10);
                    stopped = true;
                    return;
                }

                Handler handler = handlers.get(command);
                if(handler != null) {
                    String resp = handler.handle(httpExchange);
                    httpExchange.sendResponseHeaders(200, resp.length());
                    OutputStream responseBody = httpExchange.getResponseBody();
                    responseBody.write(resp.getBytes());
                    responseBody.close();
                }

                System.out.println("Help");
                String resp = "Custom Application Master \n"
                              + "    Commands:\n"
                              + "        stop - to stop am";
                httpExchange.sendResponseHeaders(200, resp.length());
                OutputStream responseBody = httpExchange.getResponseBody();
                responseBody.write(resp.getBytes());
                responseBody.close();
            }
        });
        httpServer.start();

        System.out.println("Started HTTP server on port " + httpServer.getAddress().getPort());
    }

    public boolean isStopped() {
        return stopped;
    }
}
