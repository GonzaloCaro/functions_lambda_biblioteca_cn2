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

public class EstudianteCrudHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

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
            if (is == null)
                throw new RuntimeException("Archivo de wallet NO encontrado: " + file);
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
                        responseBody = obtenerEstudiantes(conn);
                        break;
                    case "POST":
                        String bodyPost = request.getBody() != null ? request.getBody() : "{}";
                        responseBody = crearEstudiante(conn, bodyPost);
                        break;
                    case "PUT":
                        String bodyPut = request.getBody() != null ? request.getBody() : "{}";
                        responseBody = actualizarEstudiante(conn, bodyPut);
                        break;
                    case "DELETE":
                        String idStr = request.getQueryStringParameters() != null
                                ? request.getQueryStringParameters().get("id")
                                : null;
                        responseBody = eliminarEstudiante(conn, idStr);
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

    private String obtenerEstudiantes(Connection conn) throws SQLException {
        String sql = "SELECT id_estudiante, rut, nombre, apellido, email, telefono FROM estudiante ORDER BY id_estudiante ASC";
        List<Estudiante> estudiantes = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                Estudiante est = new Estudiante();
                est.id_estudiante = rs.getInt("id_estudiante");
                est.rut = rs.getString("rut");
                est.nombre = rs.getString("nombre");
                est.apellido = rs.getString("apellido");
                est.email = rs.getString("email");
                est.telefono = rs.getString("telefono");
                estudiantes.add(est);
            }
        }
        return gson.toJson(estudiantes);
    }

    private String crearEstudiante(Connection conn, String requestBody) throws SQLException {
        Estudiante est = gson.fromJson(requestBody, Estudiante.class);
        String sql = "INSERT INTO estudiante (rut, nombre, apellido, email, telefono) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, est.rut);
            pstmt.setString(2, est.nombre);
            pstmt.setString(3, est.apellido);
            pstmt.setString(4, est.email);
            pstmt.setString(5, est.telefono);
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Estudiante creado exitosamente\"}";
    }

    private String actualizarEstudiante(Connection conn, String requestBody) throws SQLException {
        Estudiante est = gson.fromJson(requestBody, Estudiante.class);
        String sql = "UPDATE estudiante SET rut = ?, nombre = ?, apellido = ?, email = ?, telefono = ? WHERE id_estudiante = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, est.rut);
            pstmt.setString(2, est.nombre);
            pstmt.setString(3, est.apellido);
            pstmt.setString(4, est.email);
            pstmt.setString(5, est.telefono);
            pstmt.setInt(6, est.id_estudiante);
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Estudiante actualizado exitosamente\"}";
    }

    private String eliminarEstudiante(Connection conn, String idStr) throws SQLException {
        if (idStr == null || idStr.isEmpty())
            throw new IllegalArgumentException("Falta ID");
        String sql = "DELETE FROM estudiante WHERE id_estudiante = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, Integer.parseInt(idStr));
            pstmt.executeUpdate();
        }
        return "{\"mensaje\": \"Estudiante eliminado exitosamente\"}";
    }

    static class Estudiante {
        Integer id_estudiante;
        String rut;
        String nombre;
        String apellido;
        String email;
        String telefono;
    }
}