package org.hackathon.healpix;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Class to connect to the PostgreSQL database
 */
public class PostgreSQLConnector {

    /**
     * Get the connection
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
    }

    // Connection
    private Connection connection;

    /**
     * Constructor
     */
    public PostgreSQLConnector() {

        connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            // Initial connection
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
