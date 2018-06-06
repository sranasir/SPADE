package spade.storage;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;


public class CompressedSQL extends CompressedStorage
{
    private Connection dbConnectionScaffold;
    private Connection dbConnectionAnnotations;

    private int cachedEntryCount = 0;
    private List<SimpleEntry<String, String>> scaffoldCache = new ArrayList<>(GLOBAL_TX_SIZE + 1);
    private List<SimpleEntry<String, byte[]>> annotationsCache = new ArrayList<>(GLOBAL_TX_SIZE + 1);
    private static String SCAFFOLD_TABLE = "scaffold";
    private static final String ANNOTATIONS_TABLE = "annotations";
    private static String[] tableColumns = {"key", "value"};
    private boolean bulkInsertion = false;    // TODO: set to false for regular insertion in scaffold


    /**
     * This method is invoked by the kernel to initialize the storage.
     *
     * @param filePath The arguments with which this storage is to be
     *                  initialized.
     * @return True if the storage was initialized successfully.
     */
    public boolean initialize(String filePath)
    {
        super.initialize(filePath);
        try
        {
            benchmarks = new PrintWriter("benchmarks/compression_time_berkeleyDB.txt", "UTF-8");
            String databaseDriver = "org.postgresql.Driver";
            String databasePrefix = "jdbc:postgresql://localhost:5432/";
            String databaseSuffix = "_4m";
            // for postgres, it is jdbc:postgres://localhost/database_name
            // for h2, it is jdbc:h2:/tmp/spade.sql
            String databaseURL = databasePrefix + "scaffold" + databaseSuffix;
            String databaseUsername = "raza";
            String databasePassword = "12345";

            Class.forName(databaseDriver).newInstance();
            dbConnectionScaffold = DriverManager.getConnection(databaseURL, databaseUsername, databasePassword);
            dbConnectionScaffold.setAutoCommit(false);

            Statement dbStatementScaffold = dbConnectionScaffold.createStatement();
            // Create scaffold table if it does not already exist
            String createScaffoldTable = "CREATE TABLE IF NOT EXISTS "
                    + "scaffold"
                    + "(key VARCHAR(256), "
                    + "value VARCHAR NOT NULL "
                    + ")";
            dbStatementScaffold.execute(createScaffoldTable);
            dbStatementScaffold.close();

            databaseURL = databasePrefix + "annotations" + databaseSuffix;
            dbConnectionAnnotations = DriverManager.getConnection(databaseURL, databaseUsername, databasePassword);
            dbConnectionAnnotations.setAutoCommit(false);

            Statement dbStatementAnnotations = dbConnectionAnnotations.createStatement();
            // Create annotations table if it does not already exist
            String createAnnotationsTable = "CREATE TABLE IF NOT EXISTS "
                    + "annotations"
                    + "(key VARCHAR(256) NOT NULL, "
                    + "value BYTEA NOT NULL "
                    + ")";
            dbStatementAnnotations.execute(createAnnotationsTable);
            dbStatementAnnotations.close();

            return true;
        }
        catch (Exception ex)
        {
            logger.log(Level.SEVERE, "Compressed Storage Initialized not successful!", ex);
            return false;
        }
    }

    /**
     * This method is invoked by the kernel to shut down the storage.
     *
     * @return True if the storage was shut down successfully.
     */
    public boolean shutdown()
    {
        try
        {
            flushBulkScaffold(true);
            dbConnectionScaffold.commit();
            dbConnectionScaffold.close();

            dbConnectionAnnotations.commit();
            dbConnectionAnnotations.close();

            return true;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Compressed Storage Shutdown not successful!", ex);
        }

        return false;
    }

    public boolean flushBulkScaffold(boolean forcedFlush)
    {
        if((cachedEntryCount > 0 && (cachedEntryCount % GLOBAL_TX_SIZE == 0)) || forcedFlush)
        {
            String scaffoldFileName = "/tmp/bulkScaffold.csv";
            try
            {
                File scaffoldFile = new File(scaffoldFileName);
                scaffoldFile.createNewFile();
                if(!(scaffoldFile.setWritable(true, false)
                        && scaffoldFile.setReadable(true, false)))
                {
                    logger.log(Level.SEVERE, "Permission denied to read/write from scaffold buffer file");
                    return false;
                }
                FileWriter fileWriter = new FileWriter(scaffoldFile);
                CSVWriter csvWriter = new CSVWriter(fileWriter);
                csvWriter.writeNext(tableColumns);
                for(Map.Entry<String, String> entry: scaffoldCache)
                {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] scaffoldEntry = {key, value};
                    csvWriter.writeNext(scaffoldEntry);
                }
                csvWriter.close();
                scaffoldCache.clear();
                String copyScaffoldQuery = "COPY "
                        + SCAFFOLD_TABLE
                        + " FROM '"
                        + scaffoldFileName
                        + "' CSV HEADER;";

                Statement statement = dbConnectionScaffold.createStatement();
                statement.execute(copyScaffoldQuery);
                statement.close();
                dbConnectionScaffold.commit();
            }
            catch(Exception ex)
            {
                logger.log(Level.SEVERE, null, ex);
                return false;
            }
        }
        return true;
    }

    private boolean processBulkScaffold(String key, String value)
    {
        try
        {
            scaffoldCache.add(new SimpleEntry<>(key, value));
            cachedEntryCount++;

            return flushBulkScaffold(false);
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error processing bulk scaffold", ex);
            return false;
        }
    }

    public boolean putScaffoldEntry(String key, String value)
    {
        try
        {
            if(value != null)
            {
                if(useAdjacencyListCache)
                {
                    adjacencyListCache.put(key, value);
                }
                if(bulkInsertion)
                {
                    return processBulkScaffold(key, value);
                }
                String sqlStatement = "INSERT INTO scaffold VALUES('" + key + "', '" + value + "');";
                Statement statement = dbConnectionScaffold.createStatement();
                statement.execute(sqlStatement);
                dbConnectionScaffold.commit();
                statement.close();
            }
            return true;
        }
        catch (Exception ex)
        {
            System.out.println("Error inserting compressed scaffold!" + ex);
            return false;
        }
    }

    public boolean flushBulkAnnotations(boolean forcedFlush)
    {
        if((cachedEntryCount > 0 && (cachedEntryCount % GLOBAL_TX_SIZE == 0)) || forcedFlush)
        {
            String annotationFileName = "/tmp/bulkAnnotation.csv";
            try
            {
                File annotationFile = new File(annotationFileName);
                annotationFile.createNewFile();
                if(!(annotationFile.setWritable(true, false)
                        && annotationFile.setReadable(true, false)))
                {
                    logger.log(Level.SEVERE, "Permission denied to read/write from annotation buffer file");
                    return false;
                }
                FileWriter fileWriter = new FileWriter(annotationFile);
                CSVWriter csvWriter = new CSVWriter(fileWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
                csvWriter.writeNext(tableColumns);
                for(Map.Entry<String, byte[]> entry: annotationsCache)
                {
                    String key = entry.getKey();
                    byte[] value = entry.getValue();
                    String[] scaffoldEntry = {key, "\\x" + Hex.encodeHexString(value)};
                    csvWriter.writeNext(scaffoldEntry);
                }
                csvWriter.close();
                annotationsCache.clear();
                String copyAnnotationsQuery = "COPY "
                        + ANNOTATIONS_TABLE
                        + " FROM '"
                        + annotationFileName
                        + "' CSV HEADER;";

                Statement statement = dbConnectionAnnotations.createStatement();
                statement.execute(copyAnnotationsQuery);
                statement.close();
                dbConnectionAnnotations.commit();
            }
            catch(Exception ex)
            {
                logger.log(Level.SEVERE, null, ex);
                return false;
            }
        }
        return true;
    }

    public boolean processBulkAnnotations(String key, byte[] value)
    {
        try
        {
            annotationsCache.add(new SimpleEntry<>(key, value));
            cachedEntryCount++;

            return flushBulkAnnotations(false);
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error processing bulk annotations", ex);
            return false;
        }
    }

    @Override
    public boolean putAnnotationEntry(String key, byte[] value)
    {
        try
        {
            if(bulkInsertion)
            {
                return processBulkAnnotations(key, value);
            }
            String sqlStatement = "INSERT INTO annotations VALUES(?, ?);";
            PreparedStatement statement = dbConnectionAnnotations.prepareStatement(sqlStatement);
            statement.setString(1, key);
            statement.setBytes(2, value);
            statement.executeUpdate();
            dbConnectionAnnotations.commit();
            statement.close();

            return true;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error putting annotation entry!", ex);
            return false;
        }
    }

    private boolean putAnnotationEntry(String key, String value)
    {
        try
        {
            String sqlStatement = "INSERT INTO annotations VALUES('" + key + "', '" + value + "');";
            Statement statement = dbConnectionAnnotations.createStatement();
            statement.execute(sqlStatement);
            dbConnectionAnnotations.commit();
            statement.close();
            return true;
        }
        catch (Exception ex)
        {
            System.out.println("Error inserting compressed annotation!" + ex);
            return false;
        }
    }

    public String getScaffoldEntry(String key)
    {
        try
        {
            getScaffolds++;
            if(useAdjacencyListCache)
            {
                String value = adjacencyListCache.get(key);
                if(value != null)
                {
                    adjacencyListCacheHits++;
                    return value;
                }
            }
            else
            {
                String sqlStatement = "SELECT * FROM scaffold WHERE key='" + key + "';";
                dbConnectionScaffold.commit();
                Statement statement = dbConnectionScaffold.createStatement();
                ResultSet result = statement.executeQuery(sqlStatement);
                String value = null;
                while(result.next())
                {
                    value = result.getString(2);
                }
                statement.close();
                return value;
            }
        }
        catch(Exception ex)
        {
            System.out.println("Error retrieving from compressed scaffold!" + ex);
        }
        return null;
    }

    public byte[] getAnnotationEntry(String key)
    {
        try
        {
            String sqlStatement = "SELECT * FROM annotations WHERE key='" + key + "';";
            dbConnectionAnnotations.commit();
            Statement statement = dbConnectionAnnotations.createStatement();
            ResultSet result = statement.executeQuery(sqlStatement);
            while (result.next())
            {
                byte[] value = result.getBytes(2);
                if(value != null)
                    return value;
                else
                    return null;
            }

            statement.close();

        }
        catch(Exception ex)
        {
            System.out.println("Error retrieving from compressed annotations!" + ex);
        }
        return null;
    }
    @Override
    public Object executeQuery(String query)
    {
        return null;
    }
}
