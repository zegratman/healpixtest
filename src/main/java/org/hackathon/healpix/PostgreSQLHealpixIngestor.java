package org.hackathon.healpix;

import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgreSQLHealpixIngestor implements Closeable {

    private static final String INSERT_STATEMENT = "INSERT INTO public.sources VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String COL_DELIMITER = ";";

    private final HealpixBase healpixbase;

    private PreparedStatement preparedStatement;

    private Integer batchSize = 0;

    private Integer currentBatchSize = 0;

    private Long ingestionTime = 0L;

    private Long healpixTime = 0L;

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

    public void ingest(String line) {
        String[] contents = line.split(COL_DELIMITER);
        try {
            Double ra = Double.valueOf(contents[4]);
            Double dec = Math.PI/2 - Double.valueOf(contents[5]);
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
            Pointing pointing = new Pointing(dec, ra);
            Long healpixId = healpixbase.ang2pix(pointing);
            preparedStatement.setLong(13, healpixbase.ang2pix(pointing));
            healpixTime += System.nanoTime() - currentTime;

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

    public Long getIngestionTime() {
        return ingestionTime;
    }

    public void close() throws IOException {
        try {
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Long getHealpixTime() {
        return healpixTime;
    }
}
