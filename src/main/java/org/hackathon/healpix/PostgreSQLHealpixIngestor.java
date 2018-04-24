package org.hackathon.healpix;

import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Ingestor class with healpix index added
 */
public class PostgreSQLHealpixIngestor implements Closeable {

    // INSERT prepared statement
    private static final String INSERT_STATEMENT = "INSERT INTO public.sources VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    // CSV column delimiter
    private static final String COL_DELIMITER = ";";

    // HEALPix library base class
    private final HealpixBase healpixbase;

    // Prepared statement
    private PreparedStatement preparedStatement;

    // Insert batch size
    private Integer batchSize = 0;

    // Current batch size of the ingestor
    private Integer currentBatchSize = 0;

    // Ingestion time
    private Long ingestionTime = 0L;

    // HEALPix computation time
    private Long healpixTime = 0L;

    /**
     * Constructor
     * @param connection the connection to the database
     * @param batchSize the size of the insert batch
     * @param nside the number of sides for HEALPix
     */
    public PostgreSQLHealpixIngestor(Connection connection, Integer batchSize, Long nside) {
        this.batchSize = batchSize <= 0 ? 1 : batchSize;
        this.healpixbase = new HealpixBase();
        try {
            preparedStatement = connection.prepareStatement(INSERT_STATEMENT);
            this.healpixbase.setNside(nside);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Ingest a line
     * @param line the line from the CSV file to ingest
     */
    public void ingest(String line) {

        // splitting line
        String[] contents = line.split(COL_DELIMITER);

        try {

            // deriving RA and DEC
            Double ra = Double.valueOf(contents[4]);
            Double theta = Math.PI/2 - Double.valueOf(contents[5]);

            // create statement
            preparedStatement.setInt(1, Integer.valueOf(contents[0]));
            preparedStatement.setInt(2, Integer.valueOf(contents[1]));
            preparedStatement.setInt(3, Integer.valueOf(contents[2]));
            preparedStatement.setBoolean(4, Boolean.valueOf(contents[3]));
            preparedStatement.setDouble(5, ra);
            preparedStatement.setDouble(6, Double.valueOf(contents[5]));
            preparedStatement.setDouble(7, Double.valueOf(contents[6]));
            preparedStatement.setDouble(8, Double.valueOf(contents[7]));
            preparedStatement.setInt(9, Integer.valueOf(contents[8]));
            preparedStatement.setDouble(10, Double.valueOf(contents[9]));
            preparedStatement.setDouble(11, Double.valueOf(contents[10]));
            preparedStatement.setDouble(12, Double.valueOf(contents[11]));

            // HEALPIX stuff
            Long currentTime = System.nanoTime();
            Pointing pointing = new Pointing(theta, ra);
            Long healpixId = healpixbase.ang2pix(pointing);
            preparedStatement.setLong(13, healpixbase.ang2pix(pointing));
            healpixTime += System.nanoTime() - currentTime;

            // adding to batch
            preparedStatement.addBatch();
            currentBatchSize++;
            if (currentBatchSize >= batchSize) {
                currentTime = System.nanoTime();
                preparedStatement.executeBatch();
                ingestionTime += System.nanoTime() - currentTime;
                System.out.println("Writing batch of data");
                currentBatchSize = 0;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Getting the ingestion time
     * @return a time reflecting the total ingestion time (calling executeBatch() on the statement)
     */
    public Long getIngestionTime() {
        return ingestionTime;
    }

    /**
     * Closing the ingestor
     * @throws IOException
     */
    public void close() throws IOException {
        try {
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the HEALPix time
     * @return the time spent to compute the index from raw RA,DEC data
     */
    public Long getHealpixTime() {
        return healpixTime;
    }
}
