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

public class PrestamoCrudHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private static final String DB_URL = "jdbc:oracle:thin:@cxtjowjkr0mdsxfa_high";
    private static final String DB_USER = "biblioteca_CN2";
    private static final String DB_PASSWORD = "Caroorion1780*";
    private static final Gson gson = new Gson();
    private static boolean walletLista = false;

    static {
        System.setProperty("java.security.egd", "file:/dev/./urandom");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("oracle.jdbc.fanEnabled", "false");
    }

    private void prepararWallet() throws Exception {
        if (walletLista) return;

        Path tmpWalletDir = Paths.get("/tmp/wallet");
        if (!Files.exists(tmpWalletDir)) {
            Files.createDirectory(tmpWalletDir);
        }

        String[] walletFiles = { "cwallet.sso", "tnsnames.ora", "sqlnet.ora", "ewallet.p12", "keystore.jks", "truststore.jks" };

        for (String file : walletFiles) {
            InputStream is = getClass().getClassLoader().getResourceAsStream("wallet/" + file);
            if (is == null) throw new RuntimeException("Archivo de wallet NO encontrado: " + file);
            Files.copy(is, tmpWalletDir.resolve(file), StandardCopyOption.REPLACE_EXISTING);
        }
        walletLista = true;
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
                        responseBody = obtenerPrestamos(conn);
                        break;
                    case "POST":
                        String bodyPost = request.getBody() != null ? request.getBody() : "{}";
                        responseBody = crearPrestamo(conn, bodyPost);
                        break;
                    case "PUT":
                        // Usaremos el PUT para "Devolver un libro"
                        String bodyPut = request.getBody() != null ? request.getBody() : "{}";
                        responseBody = devolverPrestamo(conn, bodyPut);
                        break;
                    case "DELETE":
                        String idStr = request.getQueryStringParameters() != null
                                ? request.getQueryStringParameters().get("id") : null;
                        responseBody = eliminarPrestamo(conn, idStr);
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

    private String obtenerPrestamos(Connection conn) throws SQLException {
        String sql = "SELECT id_prestamo, id_estudiante, id_libro, fecha_prestamo, fecha_limite, fecha_devolucion, estado FROM prestamo ORDER BY id_prestamo ASC";
        List<Prestamo> prestamos = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                Prestamo p = new Prestamo();
                p.id_prestamo = rs.getInt("id_prestamo");
                p.id_estudiante = rs.getInt("id_estudiante");
                p.id_libro = rs.getInt("id_libro");
                p.fecha_prestamo = rs.getDate("fecha_prestamo") != null ? rs.getDate("fecha_prestamo").toString() : null;
                p.fecha_limite = rs.getDate("fecha_limite") != null ? rs.getDate("fecha_limite").toString() : null;
                p.fecha_devolucion = rs.getDate("fecha_devolucion") != null ? rs.getDate("fecha_devolucion").toString() : null;
                p.estado = rs.getString("estado");
                prestamos.add(p);
            }
        }
        return gson.toJson(prestamos);
    }

    private String crearPrestamo(Connection conn, String requestBody) throws SQLException {
        Prestamo p = gson.fromJson(requestBody, Prestamo.class);
        String sql = "INSERT INTO prestamo (id_estudiante, id_libro, fecha_limite) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, p.id_estudiante);
            pstmt.setInt(2, p.id_libro);
            // Convierte el string "YYYY-MM-DD" del JSON a Date de SQL
            pstmt.setDate(3, Date.valueOf(p.fecha_limite)); 
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Préstamo registrado exitosamente\"}";
    }

    private String devolverPrestamo(Connection conn, String requestBody) throws SQLException {
        Prestamo p = gson.fromJson(requestBody, Prestamo.class);
        String sql = "UPDATE prestamo SET estado = 'DEVUELTO', fecha_devolucion = SYSDATE WHERE id_prestamo = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, p.id_prestamo);
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Libro devuelto y préstamo actualizado exitosamente\"}";
    }

    private String eliminarPrestamo(Connection conn, String idStr) throws SQLException {
        if (idStr == null || idStr.isEmpty()) throw new IllegalArgumentException("Falta ID");
        String sql = "DELETE FROM prestamo WHERE id_prestamo = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, Integer.parseInt(idStr));
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Préstamo eliminado exitosamente\"}";
    }

    static class Prestamo {
        Integer id_prestamo;
        Integer id_estudiante;
        Integer id_libro;
        String fecha_prestamo;
        String fecha_limite;
        String fecha_devolucion;
        String estado;
    }
}