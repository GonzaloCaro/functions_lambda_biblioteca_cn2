package com.functions.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.google.gson.Gson;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LibroCrudHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private static final String DB_URL = "jdbc:oracle:thin:@cxtjowjkr0mdsxfa_high";
    private static final String DB_USER = "biblioteca_CN2";
    private static final String DB_PASSWORD = "Caroorion1780*";
    private static final Gson gson = new Gson();

    private static boolean walletLista = false;

    // Configuracion AWS
    static {
        System.setProperty("java.security.egd", "file:/dev/./urandom");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("oracle.jdbc.fanEnabled", "false");
    }

    private void prepararWallet() throws Exception {
        if (walletLista)
            return;

        Path tmpWalletDir = Paths.get("/tmp/wallet");
        if (!Files.exists(tmpWalletDir)) {
            Files.createDirectory(tmpWalletDir);
        }

        String[] walletFiles = { "cwallet.sso", "tnsnames.ora", "sqlnet.ora", "ewallet.p12", "keystore.jks",
                "truststore.jks" };

        for (String file : walletFiles) {
            InputStream is = getClass().getClassLoader().getResourceAsStream("wallet/" + file);
            if (is == null) {
                throw new RuntimeException("Archivo de wallet NO encontrado: " + file);
            }
            Files.copy(is, tmpWalletDir.resolve(file), StandardCopyOption.REPLACE_EXISTING);
        }
        walletLista = true;
        System.out.println("Wallet extraída correctamente a /tmp/wallet");
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {

        String httpMethod = "GET";
        if (request.getRequestContext() != null && request.getRequestContext().getHttp() != null) {
            httpMethod = request.getRequestContext().getHttp().getMethod();
        }

        APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        response.setHeaders(headers);

        try {
            prepararWallet();

            Properties props = new Properties();
            props.put("user", DB_USER);
            props.put("password", DB_PASSWORD);
            props.put("oracle.net.tns_admin", "/tmp/wallet");
            props.put("oracle.net.wallet_location", "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=/tmp/wallet)))");

            try (Connection conn = DriverManager.getConnection(DB_URL, props)) {

                String responseBody = "";

                switch (httpMethod.toUpperCase()) {
                    case "GET":
                        responseBody = obtenerLibros(conn);
                        break;
                    case "POST":
                        String body = request.getBody() != null ? request.getBody() : "{}";
                        responseBody = crearLibro(conn, body);
                        break;
                    case "PUT":
                        responseBody = actualizarLibro(conn, request.getBody());
                        break;
                    case "DELETE":
                        String idStr = request.getQueryStringParameters() != null
                                ? request.getQueryStringParameters().get("id")
                                : null;
                        responseBody = eliminarLibro(conn, idStr);
                        break;
                    default:
                        response.setStatusCode(405);
                        response.setBody("{\"mensaje\": \"Método HTTP no permitido\"}");
                        return response;
                }

                response.setStatusCode(200);
                response.setBody(responseBody);
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatusCode(500);
            response.setBody("{\"error\": \"" + e.getMessage().replace("\"", "\\\"") + "\"}");
        }

        return response;
    }

    // --- CRUD ---

    private String obtenerLibros(Connection conn) throws SQLException {
        String sql = "SELECT id_libro, titulo, autor, isbn, anio_publicacion, stock FROM libro ORDER BY id_libro ASC";
        List<Libro> libros = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                Libro libro = new Libro();
                libro.id_libro = rs.getInt("id_libro");
                libro.titulo = rs.getString("titulo");
                libro.autor = rs.getString("autor");
                libro.isbn = rs.getString("isbn");
                libro.anio_publicacion = rs.getInt("anio_publicacion");
                libro.stock = rs.getInt("stock");
                libros.add(libro);
            }
        }
        return gson.toJson(libros);
    }

    private String crearLibro(Connection conn, String requestBody) throws SQLException {
        Libro nuevoLibro = gson.fromJson(requestBody, Libro.class);
        String sql = "INSERT INTO libro (titulo, autor, isbn, anio_publicacion, stock) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, nuevoLibro.titulo);
            pstmt.setString(2, nuevoLibro.autor);
            pstmt.setString(3, nuevoLibro.isbn);
            if (nuevoLibro.anio_publicacion != null) {
                pstmt.setInt(4, nuevoLibro.anio_publicacion);
            } else {
                pstmt.setNull(4, Types.INTEGER);
            }
            pstmt.setInt(5, nuevoLibro.stock != null ? nuevoLibro.stock : 1);

            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Libro creado exitosamente\"}";
    }

    private String actualizarLibro(Connection conn, String requestBody) throws SQLException {
        Libro libro = gson.fromJson(requestBody, Libro.class);
        String sql = "UPDATE libro SET titulo = ?, autor = ?, isbn = ?, anio_publicacion = ?, stock = ? WHERE id_libro = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, libro.titulo);
            pstmt.setString(2, libro.autor);
            pstmt.setString(3, libro.isbn);
            pstmt.setInt(4, libro.anio_publicacion);
            pstmt.setInt(5, libro.stock);
            pstmt.setInt(6, libro.id_libro);
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Libro actualizado exitosamente\"}";
    }

    private String eliminarLibro(Connection conn, String idStr) throws SQLException {
        if (idStr == null || idStr.isEmpty()) {
            throw new IllegalArgumentException("Se requiere el ID del libro en la URL (?id=X) para eliminar");
        }
        String sql = "DELETE FROM libro WHERE id_libro = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, Integer.parseInt(idStr));
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Libro eliminado exitosamente\"}";
    }

    // --- CLASE PARA EL JSON ---
    static class Libro {
        Integer id_libro;
        String titulo;
        String autor;
        String isbn;
        Integer anio_publicacion;
        Integer stock;
    }
}