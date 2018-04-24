package org.hackathon.healpix;

import org.postgresql.PGConnection;
import org.postgresql.jdbc.PgConnection;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLConnector {

    public Connection getConnection() {
        return connection;
    }

    private Connection connection;

    public PostgreSQLConnector() {

        connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/hackathon",
                            "vagrant", "vagrant");
            connection.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(-1);
        }
        System.out.println("Opened database successfully");

    }
}
