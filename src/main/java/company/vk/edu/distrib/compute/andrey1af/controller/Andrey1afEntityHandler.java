package company.vk.edu.distrib.compute.andrey1af.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Andrey1afEntityHandler implements HttpHandler {
    private final Dao<byte[]> dao;

    public Andrey1afEntityHandler(Dao<byte[]> dao) {
        this.dao = Objects.requireNonNull(dao);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null || id.isBlank()) {
            sendEmpty(exchange, 400);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> sendEmpty(exchange, 405);
        }
    }

    private String getId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }

        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                return param.substring(3);
            }
        }
        return null;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try (exchange) {
            try {
                byte[] value = dao.get(id);
                exchange.sendResponseHeaders(200, value.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(value);
                }
            } catch (NoSuchElementException e) {
                sendEmpty(exchange, 404);
            }
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendEmpty(exchange, 202);
    }

    private void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
        exchange.close();
    }
}
