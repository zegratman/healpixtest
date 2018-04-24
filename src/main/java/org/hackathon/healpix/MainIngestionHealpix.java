package org.hackathon.healpix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Entry point to ingest data adding HEALPix index on-the-fly
 */
public class MainIngestionHealpix {

    /**
     * Entry point
     * @param args
     */
    public static void main(String[] args) {

        // Create connector to the database
        PostgreSQLConnector connector = new PostgreSQLConnector();

        // Create ingestor
        PostgreSQLHealpixIngestor ingestor = new PostgreSQLHealpixIngestor(connector.getConnection(), 10000, 536870912L);

        try {

            // Read the input file line by line and ingest
            BufferedReader bfr = new BufferedReader(new FileReader(new File("src/main/resources/hackathon_dump.csv")));
            String line = null;
            while ((line = bfr.readLine()) != null) {
                ingestor.ingest(line);
            }
            bfr.close();

            // Prompt for results
            System.out.println("HEALPix Time : " + ingestor.getHealpixTime() + " ns");
            System.out.println("Ingestion Time : " + ingestor.getIngestionTime() + " ns");

            // Closing ingestor
            ingestor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // Commiting and closing
            connector.getConnection().commit();
            connector.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
