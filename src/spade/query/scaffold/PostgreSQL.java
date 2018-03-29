package spade.query.scaffold;


import au.com.bytecode.opencsv.CSVWriter;
import com.mysql.jdbc.StringUtils;
import spade.core.AbstractEdge;
import spade.core.Graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.AbstractMap.SimpleEntry;

import static spade.core.AbstractStorage.DIRECTION_ANCESTORS;
import static spade.core.AbstractStorage.DIRECTION_DESCENDANTS;
import static spade.core.Kernel.CONFIG_PATH;
import static spade.core.Kernel.FILE_SEPARATOR;

public class PostgreSQL extends Scaffold
{
    private Logger logger = Logger.getLogger(Scaffold.class.getName());
    private Connection dbConnection;
    private static String PARENTS_TABLE = "parents";
    private static String CHILDREN_TABLE = "children";
    private static String HASH = "hash";
    private static String PARENT_HASH = "parenthash";
    private static String CHILD_HASH = "childhash";

    private boolean bulkUpload = true;
    List<AbstractMap.SimpleEntry<String, String>> parentsCache = new ArrayList<>(GLOBAL_TX_SIZE + 1);
    List<AbstractMap.SimpleEntry<String, String>> childrenCache = new ArrayList<>(GLOBAL_TX_SIZE + 1);
//    List<AbstractMap.SimpleEntry<String, String>> parentsCache = new ArrayList<>(GLOBAL_TX_SIZE + 1);
//    private static Map<String, String> childrenCache = new HashMap<>(GLOBAL_TX_SIZE+1, 1);
    private static String[] parentsCacheColumns = {HASH, PARENT_HASH};
    private static String[] childrenCacheColumns = {HASH, CHILD_HASH};

    /**
     * This method is invoked by the kernel to initialize the storage.
     *
     * @param arguments The directory path of the scaffold storage.
     * @return True if the storage was initialized successfully.
     */
    @Override
    public boolean initialize(String arguments)
    {
        Properties databaseConfigs = new Properties();
        String configFile = CONFIG_PATH + FILE_SEPARATOR + "spade.storage.PostgreSQL.config";
        try
        {
            databaseConfigs.load(new FileInputStream(configFile));
        }
        catch(IOException ex)
        {
            logger.log(Level.SEVERE, "Loading PostgreSQL configurations from file unsuccessful!", ex);
            return false;
        }

        try
        {
            String databaseURL = "scaffold";
            databaseURL = databaseConfigs.getProperty("databaseURLPrefix") + databaseURL;
            String databaseUsername = "raza";
            String databasePassword = "12345";

            Class.forName(databaseConfigs.getProperty("databaseDriver")).newInstance();
            dbConnection = DriverManager.getConnection(databaseURL, databaseUsername, databasePassword);
            dbConnection.setAutoCommit(false);
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Unable to create PostgreSQL scaffold instance!", ex);
            return false;
        }

        try
        {
            Statement dbStatement = dbConnection.createStatement();
            // Create parent table if it does not already exist
            String createParentTable = "CREATE TABLE IF NOT EXISTS "
                    + PARENTS_TABLE
                    + "(\"" + HASH + "\" "
                    + "UUID "
                    + ", "
                    + "\"" + PARENT_HASH + "\" "
                    + "UUID "
                    + ")";
            dbStatement.execute(createParentTable);

            // Create parent table if it does not already exist
            String createChildrenTable = "CREATE TABLE IF NOT EXISTS "
                    + CHILDREN_TABLE
                    + "(\"" + HASH + "\" "
                    + "UUID "
                    + ", "
                    + "\"" + CHILD_HASH + "\" "
                    + "UUID "
                    + ")";
            dbStatement.execute(createChildrenTable);

            dbStatement.close();
            globalTxCheckin(true);

            return true;

        }
        catch (Exception ex)
        {
            logger.log(Level.SEVERE, "Unable to initialize scaffold successfully!", ex);
            return false;
        }
    }

    @Override
    public void globalTxCheckin(boolean forcedFlush)
    {
        if ((globalTxCount % GLOBAL_TX_SIZE == 0) || (forcedFlush))
        {
            try
            {
                flushBulkScaffold(true);
                dbConnection.commit();
                globalTxCount = 0;
            }
            catch(SQLException ex)
            {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            globalTxCount++;
        }
    }

    /**
     * This method is invoked by the AbstractStorage to shut down the storage.
     *
     * @return True if scaffold was shut down successfully.
     */
    @Override
    public boolean shutdown()
    {
        try
        {
            dbConnection.commit();
            dbConnection.close();
        }
        catch (Exception ex)
        {
            logger.log(Level.SEVERE, "Unable to shut down scaffold properly!", ex);
            return false;
        }
        return true;
    }

    @Override
    public Set<String> getChildren(String parentHash)
    {
        try
        {
            String queryString = "SELECT childhash FROM children WHERE hash = '" + parentHash + "'";
            Statement statement = dbConnection.createStatement();
            ResultSet result = statement.executeQuery(queryString);
            Set<String> childrenHashes = new HashSet<>();
            while (result.next())
            {
                String value = result.getString(1);
                if (!StringUtils.isNullOrEmpty(value))
                {
                    childrenHashes.add(value);
                }
            }
            return childrenHashes;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Scaffold Get Children error!", ex);
        }

        return null;
    }

    @Override
    public Set<String> getParents(String childHash)
    {
        try
        {
            String queryString = "SELECT parenthash FROM parents WHERE hash = '" + childHash + "'";
            Statement statement = dbConnection.createStatement();
            ResultSet result = statement.executeQuery(queryString);
            Set<String> parentHashes = new HashSet<>();
            while (result.next())
            {
                String value = result.getString(1);
                if (!StringUtils.isNullOrEmpty(value))
                {
                    parentHashes.add(value);
                }
            }
            return parentHashes;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Scaffold Get Parents error!", ex);
        }

        return null;
    }

    @Override
    public Set<String> getNeighbors(String hash)
    {
        return null;
    }

    @Override
    public Map<String, Set<String>> getLineage(String hash, String direction, int maxDepth)
    {
        try
        {
            String tableName = (direction.startsWith("a"))? "parents" : "children";
            String getVertexQuery = "SELECT * FROM " + tableName + " WHERE hash = '" + hash + "'";
            Statement statement = dbConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(getVertexQuery);
            if(resultSet.next())
            {
                Set<String> remainingVertices = new HashSet<>();
                Set<String> visitedVertices = new HashSet<>();
                Map<String, Set<String>> lineageMap = new HashMap<>();
                remainingVertices.add(hash);
                int current_depth = 0;
                while(!remainingVertices.isEmpty() && current_depth < maxDepth)
                {
                    visitedVertices.addAll(remainingVertices);
                    Set<String> currentSet = new HashSet<>();
                    for(String current_hash: remainingVertices)
                    {
                        Set<String> neighbors = null;
                        if(DIRECTION_ANCESTORS.startsWith(direction.toLowerCase()))
                            neighbors = getParents(current_hash);
                        else if(DIRECTION_DESCENDANTS.startsWith(direction.toLowerCase()))
                            neighbors = getChildren(current_hash);

                        if(neighbors != null)
                        {
                            lineageMap.put(current_hash, neighbors);
                            for(String vertexHash: neighbors)
                            {
                                if(!visitedVertices.contains(vertexHash))
                                {
                                    currentSet.addAll(neighbors);
                                }
                            }
                        }
                    }
                    remainingVertices.clear();
                    remainingVertices.addAll(currentSet);
                    current_depth++;
                }

                return lineageMap;
            }
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Scaffold Get Lineage error!", ex);
        }


        return null;
    }

    @Override
    public Map<String, Set<String>> getPaths(String source_hash, String destination_hash, int maxLength)
    {
        return null;
    }

    /**
     * This function inserts hashes of the end vertices of given edge
     * into the scaffold storage.
     *
     * @param incomingEdge edge whose end points to insert into the storage
     * @return returns true if the insertion is successful. Insertion is considered
     * not successful if the vertex is already present in the storage.
     */
    @Override
    public boolean insertEntry(AbstractEdge incomingEdge)
    {
        if(bulkUpload)
        {
            return processBulkScaffold(incomingEdge);
        }
        try
        {
            StringBuilder parentStringBuilder = new StringBuilder(100);
            parentStringBuilder.append("INSERT INTO ");
            parentStringBuilder.append(PARENTS_TABLE);
            parentStringBuilder.append("(");
            parentStringBuilder.append("\"");
            parentStringBuilder.append(HASH);
            parentStringBuilder.append("\", ");
            parentStringBuilder.append(PARENT_HASH);
            parentStringBuilder.append(") ");
            parentStringBuilder.append("VALUES(");
            parentStringBuilder.append("'");
            parentStringBuilder.append(incomingEdge.getChildVertex().bigHashCode());
            parentStringBuilder.append("', ");
            parentStringBuilder.append("'");
            parentStringBuilder.append(incomingEdge.getParentVertex().bigHashCode());
            parentStringBuilder.append("');");

            Statement statement = dbConnection.createStatement();
            statement.execute(parentStringBuilder.toString());

            StringBuilder childrenStringBuilder = new StringBuilder(100);
            childrenStringBuilder.append("INSERT INTO ");
            childrenStringBuilder.append(CHILDREN_TABLE);
            childrenStringBuilder.append("(");
            childrenStringBuilder.append("\"");
            childrenStringBuilder.append(HASH);
            childrenStringBuilder.append("\", ");
            childrenStringBuilder.append(CHILD_HASH);
            childrenStringBuilder.append(") ");
            childrenStringBuilder.append("VALUES(");
            childrenStringBuilder.append("'");
            childrenStringBuilder.append(incomingEdge.getParentVertex().bigHashCode());
            childrenStringBuilder.append("', ");
            childrenStringBuilder.append("'");
            childrenStringBuilder.append(incomingEdge.getChildVertex().bigHashCode());
            childrenStringBuilder.append("');");

            statement.execute(childrenStringBuilder.toString());
            statement.close();
            globalTxCheckin(false);
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error inserting data into scaffold");
            return false;
        }

        return true;
    }

    private boolean processBulkScaffold(AbstractEdge incomingEdge)
    {
        try
        {
            parentsCache.add(new SimpleEntry<>(incomingEdge.getChildVertex().bigHashCode(), incomingEdge.getParentVertex().bigHashCode()));
            childrenCache.add(new SimpleEntry<>(incomingEdge.getParentVertex().bigHashCode(), incomingEdge.getChildVertex().bigHashCode()));
            globalTxCount++;

            return flushBulkScaffold(false);
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error inserting in scaffold cache!", ex);
        }

        return false;
    }

    private boolean flushBulkScaffold(boolean forcedFlush)
    {
        // bulk inserts into Postgres via COPY command
        try
        {
            // creating parents
            if(( (globalTxCount > 0) && (globalTxCount % GLOBAL_TX_SIZE == 0) ) || forcedFlush)
            {
                String parentsFileName = "/tmp/bulk_parents.csv";
                File parentsFile = new File(parentsFileName);
                parentsFile.getParentFile().mkdirs();
                parentsFile.createNewFile();
                if(!(parentsFile.setWritable(true, false)
                        && parentsFile.setReadable(true, false)))
                {
                    logger.log(Level.SEVERE, "Permission denied to read/write from scaffold parent cache files!");
                    return false;
                }
                FileWriter parentsFileWriter = new FileWriter(parentsFile);
                CSVWriter parentsCSVWriter = new CSVWriter(parentsFileWriter);
                parentsCSVWriter.writeNext(parentsCacheColumns);
                for(Map.Entry<String, String> parentEntry: parentsCache)
                {
                    String hash = parentEntry.getKey();
                    String parenthash = parentEntry.getValue();
                    String[] parentEntryValues = {hash, parenthash};
                    parentsCSVWriter.writeNext(parentEntryValues);
                }
                parentsCSVWriter.close();
                parentsCache.clear();
                String copyParentsString = "COPY "
                        + PARENTS_TABLE
                        + " FROM '"
                        + parentsFileName
                        + "' CSV HEADER";

                // creating children
                String childrenFileName = "/tmp/bulk_children.csv";
                File childrenFile = new File(childrenFileName);
                childrenFile.getParentFile().mkdirs();
                childrenFile.createNewFile();
                if(!(childrenFile.setWritable(true, false)
                        && childrenFile.setReadable(true, false)))
                {
                    logger.log(Level.SEVERE, "Permission denied to read/write from scaffold children cache files!");
                    return false;
                }
                FileWriter childrenFileWriter = new FileWriter(childrenFile);
                CSVWriter childrenCSVWriter = new CSVWriter(childrenFileWriter);
                childrenCSVWriter.writeNext(childrenCacheColumns);
                for(Map.Entry<String, String> childEntry: childrenCache)
                {
                    String hash = childEntry.getKey();
                    String childhash = childEntry.getValue();
                    String[] childEntryValues = {hash, childhash};
                    childrenCSVWriter.writeNext(childEntryValues);
                }
                childrenCSVWriter.close();
                childrenCache.clear();
                String copyChildrenString = "COPY "
                        + CHILDREN_TABLE
                        + " FROM '"
                        + childrenFileName
                        + "' CSV HEADER";

                Statement statement = dbConnection.createStatement();
                statement.execute(copyParentsString);
                statement.execute(copyChildrenString);
                statement.close();
                dbConnection.commit();
            }
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error flushing scaffold cache to storage!", ex);
            return false;
        }

        return true;
    }

    @Override
    public Graph queryManager(Map<String, List<String>> params)
    {
        return null;
    }
}
