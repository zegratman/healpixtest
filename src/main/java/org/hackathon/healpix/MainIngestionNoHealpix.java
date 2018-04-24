package org.hackathon.healpix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Entry point to ingest data without HEALPix
 */
public class MainIngestionNoHealpix {

    /**
     * Entry point
     * @param args params
     */
    public static void main(String[] args) {

        // Connector to database
        PostgreSQLConnector connector = new PostgreSQLConnector();

        // Ingestor
        PostgreSQLIngestor ingestor = new PostgreSQLIngestor(connector.getConnection(), 10000);

        try {

            // Reading line by line
            BufferedReader bfr = new BufferedReader(new FileReader(new File("src/main/resources/hackathon_dump.csv")));
            String line = null;
            while ((line = bfr.readLine()) != null) {
                ingestor.ingest(line);
            }
            bfr.close();

            // Giving the ingestion time
            System.out.println("Ingestion Time : " + ingestor.getIngestionTime() + " ns");

            // Closing ingestor
            ingestor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // Committing and close
            connector.getConnection().commit();
            connector.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
