package org.hackathon.healpix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

public class MainIngestionHealpix {

    public static void main(String[] args) {

        PostgreSQLConnector connector = new PostgreSQLConnector();

        PostgreSQLHealpixIngestor ingestor = new PostgreSQLHealpixIngestor(connector.getConnection(), 10000, 536870912L);

        try {

            BufferedReader bfr = new BufferedReader(new FileReader(new File("src/main/resources/hackathon_dump.csv")));
            String line = null;
            while ((line = bfr.readLine()) != null) {
                ingestor.ingest(line);
            }

            bfr.close();

            System.out.println("HEALPix Time : " + ingestor.getHealpixTime() + " ns");
            System.out.println("Ingestion Time : " + ingestor.getIngestionTime() + " ns");

            ingestor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            connector.getConnection().commit();
            connector.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
