package spade.storage;

import com.sleepycat.je.utilint.Pair;
import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressedStorageSQL extends AbstractStorage
{
    private Connection dbConnectionScaffold;
    private Connection dbConnectionAnnotations;

    private static String directoryPath = null;
    static Integer nextVertexID;
    public static Integer W;
    public static Integer L;
    static Deflater compresser;
    static Vector<String> alreadyRenamed;
    static Map<String, Integer> hashToID;
    static Map<Integer, String> idToHash;
    static int edgesInMemory;
    static int maxEdgesInMemory = 10000;
    Map<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> scaffoldInMemory;
    Map<String, String> scaffoldCache;
    private static final Logger logger = Logger.getLogger(CompressedStorage.class.getName());
    public long clockScaffold;
    public long clockAnnotations;
    public static PrintWriter benchmarks;
    public static long scaffoldTime;
    public static long annotationsTime;
    public static int countUpdates;

    /**
     * This method is invoked by the kernel to initialize the storage.
     *
     * @param filePath The arguments with which this storage is to be
     *                  initialized.
     * @return True if the storage was initialized successfully.
     */
    public boolean initialize(String filePath) {
        //clock = System.currentTimeMillis();
        countUpdates = 0;
        annotationsTime = 0;
        scaffoldTime = 0;
        clockScaffold = 0;
        clockAnnotations = 0;
        scaffoldInMemory = new HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>();
        scaffoldCache = new HashMap<>();
        edgesInMemory = 0;
        hashToID = new HashMap<String, Integer>();
        idToHash = new HashMap<Integer, String>();
        alreadyRenamed = new Vector<String>();
        compresser = new Deflater(Deflater.BEST_COMPRESSION);
        W=10;
        L=5;
        nextVertexID = 0;
        try
        {
            benchmarks = new PrintWriter("benchmarks/compression_time_berkeleyDB.txt", "UTF-8");
            String databaseDriver = "org.postgresql.Driver";
            String databasePrefix = "jdbc:postgresql://localhost:5432/";
            // for postgres, it is jdbc:postgres://localhost/database_name
            // for h2, it is jdbc:h2:/tmp/spade.sql
            String databaseURL = databasePrefix + "scaffold";
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

            databaseURL = databasePrefix + "annotations";
            dbConnectionAnnotations = DriverManager.getConnection(databaseURL, databaseUsername, databasePassword);
            dbConnectionAnnotations.setAutoCommit(false);

            Statement dbStatementAnnotations = dbConnectionAnnotations.createStatement();
            // Create annotations table if it does not already exist
            String createAnnotationsTable = "CREATE TABLE IF NOT EXISTS "
                    + "annotations"
                    + "(key VARCHAR(256) NOT NULL, "
                    + "value VARCHAR NOT NULL "
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
        System.out.println("Average time to put 10000 edges in the annotations storage : " + annotationsTime);
        System.out.println("Average time to put 10000 edges in the scaffold strage: " + scaffoldTime);
        try
        {
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


    /**
     * Create a hash map linking each node to its full list of ancestors and its full list of successors from the text file listing edges and vertexes issued by SPADE.
     * @param textfile The name of the text file issued by SPADE without the extension
     * @return HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> maps each node to its list of ancestors and its list of successors. The first set of the pair is the ancestors list, the second one is the successors list.
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> createAncestorSuccessorList( String textfile) throws FileNotFoundException, UnsupportedEncodingException {
        File file = new File(textfile + ".txt");
        Scanner sc = new Scanner(file);
        HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> ancestorsSuccessors = new  HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>();
        while(sc.hasNextLine()){
            String aux = sc.nextLine();
            if(aux.substring(0,4).equals("EDGE")){
                String node_s = aux.substring(6, aux.indexOf(" ->"));
                Integer node = Integer.parseInt(node_s);
                String successor_s = aux.substring(aux.indexOf(" -> ")+4, aux.indexOf("): {"));
                Integer successor = Integer.parseInt(successor_s);
                if(ancestorsSuccessors.containsKey(node)) {
                    Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = ancestorsSuccessors.get(node);
                    partialLists.second().add(successor);
                    ancestorsSuccessors.replace(node, partialLists);
                } else {
                    SortedSet<Integer> partialSuccessorList = new TreeSet<Integer>();
                    SortedSet<Integer> partialAncestorList = new TreeSet<Integer>();
                    partialSuccessorList.add(successor);
                    Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = new Pair<SortedSet<Integer>, SortedSet<Integer>>(partialAncestorList, partialSuccessorList);
                    ancestorsSuccessors.put(node, partialLists);
                }
                if(ancestorsSuccessors.containsKey(successor)) {
                    Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = ancestorsSuccessors.get(successor);
                    partialLists.first().add(node);
                    ancestorsSuccessors.replace(successor, partialLists);
                } else {
                    SortedSet<Integer> partialSuccessorList = new TreeSet<Integer>();
                    SortedSet<Integer> partialAncestorList = new TreeSet<Integer>();
                    partialAncestorList.add(node);
                    Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = new Pair<SortedSet<Integer>, SortedSet<Integer>>(partialAncestorList, partialSuccessorList);
                    ancestorsSuccessors.put(successor, partialLists);
                }
            }
        }
        sc.close();
        //write it in a file
        PrintWriter writer = new PrintWriter(textfile + "_ancestor_successor.txt", "UTF-8");
        Set<HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entries = ancestorsSuccessors.entrySet();
        for (HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> node : entries){
            writer.println(node.getKey().toString() + " " + node.getValue().first().toString() + " " + node.getValue().second().toString());
        }
        writer.close();
        return ancestorsSuccessors;
    }

    /**
     * Encode the list of ancestors and the list of successors of each node in a file called textfile_ancestor_successor_compressed.txt
     * @param ancestorSuccessorList
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public void encodeAncestorsSuccessors(HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> ancestorSuccessorList) throws FileNotFoundException, UnsupportedEncodingException {
        Set<HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entries = ancestorSuccessorList.entrySet();
        // compress each node
        for (HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToCompress : entries){
            encodeAncestorsSuccessors(nodeToCompress);
            //System.out.println("Node " + nodeToCompress.getKey() + " encoded.");
        }
    }

    public boolean encodeAncestorsSuccessors(HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToCompress) {
        //find reference node
        Integer id = nodeToCompress.getKey();
        //System.out.println(id);
        SortedSet<Integer> ancestors = nodeToCompress.getValue().first();
        SortedSet<Integer> successors = nodeToCompress.getValue().second();
        //NodeLayerAncestorSuccessor currentNode = new NodeLayerAncestorSuccessor(id, ancestors, successors);
        Pair<Integer, Integer> maxNodesInCommonAncestor = new Pair<Integer, Integer>(0,0);
        Pair<Integer, Integer> maxNodesInCommonSuccessor = new Pair<Integer, Integer>(0,0); //first integer is the max of nodes in common, the second one is the number of 0 in the corresponding bit list
        //NodeLayerAncestorSuccessor referenceAncestor = currentNode;
        String bitlistAncestor = "";
        int layerAncestor = 1;
        Integer referenceAncestor = -1;
        SortedSet<Integer> referenceAncestorList = new TreeSet<Integer>();
        //NodeLayerAncestorSuccessor referenceSuccessor = currentNode;
        String bitlistSuccessor = "";
        int layerSuccessor = 1;
        Integer referenceSuccessor = -1;
        SortedSet<Integer> referenceSuccessorList = new TreeSet<Integer>();
        //Iterator<NodeLayerAncestorSuccessor> iteratorPossibleReference = lastNodesSeen.iterator();
        //while (iteratorPossibleReference.hasNext()){
        //System.out.println("step 1");
        for (Integer possibleReferenceID = Math.max(0, id - W); possibleReferenceID<id; possibleReferenceID ++){
            //for each node in the W last nodes seen, compute the proximity, i.e. the number of successors of the current node that also are successors of the possibleReference node.
            Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> asl = uncompressAncestorsSuccessorsWithLayer(possibleReferenceID, true, true);
            if(asl.first().first() < L) {
                //System.out.println("step 1.1");
                Pair<Pair<Integer, Integer>, String> nodesInCommonAncestor = commonNodes(asl.first().second(), ancestors);
                int numberOfOneAncestor = nodesInCommonAncestor.first().first();
                int numberOfZeroAncestor = nodesInCommonAncestor.first().second();
                int maxNumberOfOneAncestor = maxNodesInCommonAncestor.first();
                int maxNumberOfZeroAncestor = maxNodesInCommonAncestor.second();
                //System.out.println("step 2");
                if (numberOfOneAncestor>maxNumberOfOneAncestor || (numberOfOneAncestor==maxNumberOfOneAncestor && numberOfZeroAncestor<maxNumberOfZeroAncestor)) {
                    maxNodesInCommonAncestor = nodesInCommonAncestor.first();
                    bitlistAncestor = nodesInCommonAncestor.second();
                    referenceAncestor = possibleReferenceID;
                    layerAncestor = asl.first().first() + 1;
                    referenceAncestorList = asl.first().second();
                    //System.out.println("step 3");
                }
            }
            //System.out.println("step 4");
            if (asl.second().first() < L) {
                //System.out.println("step 4.1");
                Pair<Pair<Integer, Integer>, String> nodesInCommonSuccessor =  commonNodes(asl.second().second(), successors);
                int numberOfOneSuccessor = nodesInCommonSuccessor.first().first();
                int numberOfZeroSuccessor = nodesInCommonSuccessor.first().second();
                int maxNumberOfOneSuccessor = maxNodesInCommonSuccessor.first();
                int maxNumberOfZeroSuccessor = maxNodesInCommonSuccessor.second();
                if (numberOfOneSuccessor>maxNumberOfOneSuccessor || (numberOfOneSuccessor==maxNumberOfOneSuccessor && numberOfZeroSuccessor<maxNumberOfZeroSuccessor)) {
                    maxNodesInCommonSuccessor = nodesInCommonSuccessor.first();
                    bitlistSuccessor = nodesInCommonSuccessor.second();
                    referenceSuccessor = possibleReferenceID;
                    layerSuccessor = asl.second().first() + 1;
                    referenceSuccessorList = asl.second().second();
                }
            }
            //System.out.println("step 5");
        }
        //System.out.println("step 6");

        //encode ancestor list
        SortedSet<Integer> remainingNodesAncestor = new TreeSet<Integer>();
        remainingNodesAncestor.addAll(ancestors);
        //encode reference
        //String encoding = id.toString() + " ";
        String encoding = layerAncestor + " ";
        if (maxNodesInCommonAncestor.first() > 0) {
            encoding = encoding + (referenceAncestor - id) + " " + bitlistAncestor + "";
            //keep only remaining nodes
            remainingNodesAncestor.removeAll(referenceAncestorList);
        } else {
            encoding = encoding + "_";
        }

        //encode consecutive nodes and delta encoding
        Integer previousNode = id;
        int countConsecutives = 0;
        for (Integer nodeID : remainingNodesAncestor) {
            Integer delta = nodeID - previousNode;
            if (delta == 1) {
                countConsecutives++;
            } else {

                if (countConsecutives > 0) {
                    encoding = encoding + ":" + countConsecutives;
                    countConsecutives = 1;
                }
                encoding = encoding + " " + delta;
                countConsecutives = 0;
            }
            previousNode = nodeID;

        }
        // encode successor list
        SortedSet<Integer> remainingNodesSuccessor = new TreeSet<Integer>();
        remainingNodesSuccessor.addAll(successors);
        //encode reference
        encoding = encoding + " / " + layerSuccessor + " ";
        if (maxNodesInCommonSuccessor.first() > 0) {
            encoding = encoding + (referenceSuccessor - id) + " " + bitlistSuccessor + "";
            //keep only remaining nodes
            remainingNodesSuccessor.removeAll(referenceSuccessorList);
        } else {
            encoding = encoding + "_ ";
        }

        //encode consecutive nodes and delta encoding
        previousNode = id;
        countConsecutives = 0;
        for (Integer nodeID : remainingNodesSuccessor) {
            Integer delta = nodeID - previousNode;
            if (delta == 1) {
                countConsecutives++;
            } else {

                if (countConsecutives > 0) {

                    encoding = encoding + ":" + countConsecutives;
                    countConsecutives = 1;
                }
                encoding = encoding + " " + delta;
                countConsecutives = 0;
            }
            previousNode = nodeID;

        }
        //System.out.println(id + "?" + encoding);
        putScaffold(id.toString(), encoding);
        //System.out.println(id + " " + encoding);
        return true;
    }

    public  Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> uncompressAncestorsSuccessorsWithLayer(
            Integer id, boolean uncompressAncestors, boolean uncompressSuccessors) {
        //System.out.println("step a");
        SortedSet<Integer> ancestors = new TreeSet<Integer>();
        SortedSet<Integer> successors = new TreeSet<Integer>();
        Integer ancestorLayer = 1;
        Integer successorLayer = 1;
        String aux = getScaffold(id.toString());
        //System.out.println("step b");
        if(aux != null && aux.contains("/")) {
            // split the line in two parts : ancestor list and successor list.
            String ancestorList = aux.substring(0, aux.indexOf('/'));
            String successorList = aux.substring(aux.indexOf('/')+2);
            ancestorLayer = Integer.parseInt(ancestorList.substring(0, ancestorList.indexOf(' ')));
            successorLayer = Integer.parseInt(successorList.substring(0, successorList.indexOf(' ')));
            ancestorList = ancestorList.substring(ancestorList.indexOf(' ') + 1);
            successorList = successorList.substring(successorList.indexOf(' ') + 1);
            //System.out.println("step c");
            if (uncompressAncestors) { //uncompressAncestors
                if(ancestorList.contains("_")) { // means there is no reference
                    //System.out.println("step d");
                    String ancestorList2 = ancestorList.substring(ancestorList.indexOf("_") + 1);
                    ancestors.addAll(uncompressRemainingNodes(id, ancestorList2));
                } else { // there is a reference that we have to uncompress
                    // uncompress the remaining Nodes
                    //System.out.println("step e");
                    String remaining = ancestorList.substring(ancestorList.indexOf(" ")+1);
                    remaining = remaining.substring(remaining.indexOf(" ")+1);
                    ancestors.addAll(uncompressRemainingNodes(id, remaining));
                    //uncompress the reference and its reference after that
                    try {
                        ancestors.addAll(uncompressReference(id, ancestorList, true));
                    } catch (UnsupportedEncodingException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            //System.out.println("step f");
            if (uncompressSuccessors) { // uncompressSuccessors
                if(successorList.contains("_")) { // means there is no reference
                    //System.out.println("step g " );
                    String successorList2 = successorList.substring(successorList.indexOf("_")+ 1);
                    successors.addAll(uncompressRemainingNodes(id, successorList2));
                } else { // there is a reference that we have to uncompress
                    // uncompress the remaining Nodes
                    //System.out.println("step h");
					/*			String remaining = successorList.substring(successorList.indexOf(" ")+1);
					remaining = remaining.substring(remaining.indexOf(" ")+1);
					System.out.println("remaining" +remaining);
					//System.out.println("step i ");
					successors.addAll(uncompressRemainingNodes(id, remaining));*/
                    //uncompress the reference and its reference after that
                    try {
                        //System.out.println("step j ");
                        successors.addAll(uncompressReference(id, successorList, false));
                        //System.out.println("step k");
                    } catch (UnsupportedEncodingException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
        //System.out.println("step k");
        Pair<Integer, SortedSet<Integer>> ancestorsAndLayer = new Pair<Integer, SortedSet<Integer>>(ancestorLayer, ancestors);
        Pair<Integer, SortedSet<Integer>> successorsAndLayer = new Pair<Integer, SortedSet<Integer>>(successorLayer, successors);
        Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> ancestorsAndSuccessors = new Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>>(ancestorsAndLayer, successorsAndLayer);
        return ancestorsAndSuccessors;
    }

    public  Pair<Pair<Integer, Integer>, String> commonNodes(SortedSet<Integer> reference,
                                                                   SortedSet<Integer> node) {
        int nodesInCommon = 0;
        int numberOfZero = 0;
        String bitlist = "";
        for (Integer successor : reference) {
            if (node.contains(successor)) {
                nodesInCommon++;
                bitlist = bitlist + "1";
            } else {
                numberOfZero++;
                bitlist = bitlist + "0";
            }
        }
        Pair<Integer, Integer> count = new Pair<Integer, Integer>(nodesInCommon, numberOfZero);

        //System.out.println("Common nodes - reference size:" + reference.size() + " node size - " + node.size() + "bitlist size:" + bitlist.length());
        return new Pair<Pair<Integer, Integer>, String>(count, bitlist);
    }

    private boolean putScaffold(String key, String value)
    {
        try
        {
            scaffoldCache.put(key, value);
            String sqlStatement = "INSERT INTO scaffold VALUES('" + key + "', '" + value + "');";
            dbConnectionScaffold.commit();
            Statement statement = dbConnectionScaffold.createStatement();
            statement.execute(sqlStatement);
            statement.close();

            return true;
        }
        catch (Exception ex)
        {
            System.out.println("Error inserting compressed scaffold!" + ex);
            return false;
        }
    }

    private boolean putAnnotation(String key, String value)
    {
        try
        {
            String sqlStatement = "INSERT INTO annotations VALUES('" + key + "', '" + value + "');";
            dbConnectionAnnotations.commit();
            Statement statement = dbConnectionAnnotations.createStatement();
            statement.execute(sqlStatement);
            statement.close();

            return true;
        }
        catch (Exception ex)
        {
            System.out.println("Error inserting compressed annotation!" + ex);
            return false;
        }
    }

    private String getScaffold(String key)
    {
        try
        {
            String value = scaffoldCache.get(key);
            if(value != null)
            {
                return value;
            }
            String sqlStatement = "SELECT * FROM scaffold WHERE key='" + key + "';";
            dbConnectionScaffold.commit();
            Statement statement = dbConnectionScaffold.createStatement();
            ResultSet result = statement.executeQuery(sqlStatement);
            while (result.next())
            {
                value = result.getString(2);
                return value;
            }

            statement.close();

        }
        catch(Exception ex)
        {
            System.out.println("Error retrieving from compressed scaffold!" + ex);
        }
        return null;
    }

    private String getAnnotation(String key)
    {
        try
        {
            String sqlStatement = "SELECT * FROM annotations WHERE key='" + key + "';";
            dbConnectionAnnotations.commit();
            Statement statement = dbConnectionAnnotations.createStatement();
            ResultSet result = statement.executeQuery(sqlStatement);
            while (result.next())
            {
                String value = result.getString(2);
                return value;
            }

            statement.close();

        }
        catch(Exception ex)
        {
            System.out.println("Error retrieving from compressed annotations!" + ex);
        }
        return null;
    }

    public  boolean putNext(String key, String newValue, String sep)
    {
        byte[] separatorInput;
        try
        {
            separatorInput = sep.getBytes("UTF-8");
            byte [] separator = new byte[separatorInput.length];
            compresser.setInput(separatorInput);
            compresser.finish();
            compresser.deflate(separator);
            String oldValue = getScaffold(key);
            String value;
            if (oldValue.length() > 0)
            {
                value = oldValue + sep + newValue;
            }
            else
            {
                value = newValue;
            }
            putScaffold(key, value);
            return true;
        }
        catch (Exception ex)
        {
            System.out.println("Error putting next entry!" + ex);
            return false;
        }

    }


    public  SortedSet<Integer> uncompressRemainingNodes(Integer nodeID, String remainingNodes){
        //System.out.println("remainingNodes :" + remainingNodes);
        SortedSet<Integer> successors = new TreeSet<Integer>();
        Integer currentID = nodeID;
        String uncompressing;
        int length;
        StringTokenizer st = new StringTokenizer(remainingNodes);
        while (st.hasMoreTokens()) {
            uncompressing = st.nextToken();
            if(uncompressing.contains(":")) { //uncompress consecutive numbers
                if(uncompressing.charAt(0) != ':'){
                    currentID += Integer.parseInt(uncompressing.substring(0, uncompressing.indexOf(':')));
                    successors.add(currentID);
                }
                length = Integer.parseInt(uncompressing.substring(uncompressing.indexOf(':')+1));
                for (int i=0; i<length; i++) {
                    currentID ++;
                    successors.add(currentID);
                }
            } else {
                currentID = currentID + Integer.parseInt(uncompressing);
                successors.add(currentID);
            }
        }
        //System.out.println("uncompressRemainingNodes : " + successors.toString());
        return successors;
    }

    private  SortedSet<Integer> uncompressReference(Integer id, String ancestorOrSuccessorList,
                                                          boolean ancestorOrSuccessor) throws UnsupportedEncodingException {
        //System.out.println("step m");

        SortedSet<Integer> list = new TreeSet<Integer>();

        StringTokenizer st = new StringTokenizer(ancestorOrSuccessorList);
        //System.out.println("ancestorOrSuccessorList :" + ancestorOrSuccessorList + " /id :" + id);
        //st.nextToken();
        String token = st.nextToken();
        //System.out.println("step n");
        Integer referenceID = id + Integer.parseInt(token);
        //System.out.println("referenceID1: " + referenceID);
        LinkedList<String> previousLayers = new LinkedList<String>();
        previousLayers.addFirst(id + " " + ancestorOrSuccessorList);
        String toUncompress;
        //System.out.println("step o");
        boolean hasReference = true;
        //System.out.println(toUncompress);
        //System.out.println(ancestorOrSuccessorList);
        while (hasReference){
            //System.out.println("step p");
            String currentLine = getScaffold(referenceID.toString());
            if(currentLine.length() > 0) {

                if (ancestorOrSuccessor) { // we want to uncompress ancestors
                    toUncompress = currentLine.substring(currentLine.indexOf(' ')+1, currentLine.indexOf("/") - 1);
                } else { // we want to uncompress successors
                    toUncompress = currentLine.substring(currentLine.indexOf("/") + 2);
                    toUncompress = toUncompress.substring(toUncompress.indexOf(' ') + 1);
                }
                //System.out.println("step q");
                //System.out.println("toUncompress:" + toUncompress);
                // System.out.println(toUncompress);
                toUncompress = referenceID + " " + toUncompress;
                previousLayers.addFirst(toUncompress);
                //System.out.println("step r");
                if (toUncompress.contains("_")) { // this is the last layer
                    hasReference = false;
                } else { // we need to go one layer further to uncompress the successors
                    String aux = toUncompress.substring(toUncompress.indexOf(" ")+1);
                    //System.out.println("toUncompress:" + toUncompress);
                    referenceID = referenceID + Integer.parseInt(aux.substring(0, aux.indexOf(" ")));
                    // System.out.println("referenceID : " + referenceID);
                    //System.out.println("step s");
                }
            } else {
                System.out.println("Data missing.");
                hasReference = false;
            }//System.out.println("step t");
        }

        // System.out.println("previousLayers: " + previousLayers.toString());
        String bitListLayer;
        String remainingNodesLayer;
        Integer layerID;
        for(String layer : previousLayers) { //find the successors of the first layer and then those of the second layer and so on...
            layerID = Integer.parseInt(layer.substring(0, layer.indexOf(" ")));
            //System.out.println("step u");
            if (layer.contains("_")) { //this is the case for the first layer only
                remainingNodesLayer = layer.substring(layer.indexOf("_")+2);
                //benchmarks.println("____ " + layer + " /// " + remainingNodesLayer);
                //System.out.println("step v");
            } else {
                // uncompress the bitlist
                remainingNodesLayer = layer.substring(layer.indexOf(" ") + 1);
                //System.out.println("remaining Nodes Layer 1: " + remainingNodesLayer);
                //// System.out.println("step 1 :" + remainingNodesLayer + "/");
                remainingNodesLayer = remainingNodesLayer.substring(remainingNodesLayer.indexOf(" ") + 1);
                //remainingNodesLayer = remainingNodesLayer.substring(remainingNodesLayer.indexOf(" ") + 1);
                //// System.out.println("step 2 :" + remainingNodesLayer + "/");
                //System.out.println("step w");
                //System.out.println("remaining Nodes Layer " + remainingNodesLayer);
                if (remainingNodesLayer.contains(" ")) {
                    bitListLayer = remainingNodesLayer.substring(0, remainingNodesLayer.indexOf(" "));
                    remainingNodesLayer = remainingNodesLayer.substring(remainingNodesLayer.indexOf(" ") + 1);
                } else {

                    bitListLayer = remainingNodesLayer;
                    remainingNodesLayer = "";
                }
                //benchmarks.println("ref:" + layer + " ////remaining:" + remainingNodesLayer + "////bitListLayer:" + bitListLayer );
                //System.out.println("bitListLayer :" + bitListLayer + "/");
                int count = 0;
                SortedSet<Integer> list2 = new TreeSet<Integer>();
                list2.addAll(list);
                //	System.out.println("step x");
                //System.out.println(bitListLayer);
                //System.out.println("BUG:" + list2.toString() + "/bit:" +  bitListLayer);
                for (Integer successor : list2) {
                    try {
                        if(bitListLayer.charAt(count) == '0') {
                            list.remove(successor);
                            //System.out.println("step y");
                        }
                    } catch (Exception ex) {
                        //System.out.println("update unsuccessful in uncompress reference :" + ex.getMessage() + "/count:" + count + "/successor.length:" + list.size() + "/list2.length:" + list2.size() + "bitListLayer.length:" + bitListLayer.length()+ "nodeID:" + layerID + "/bitListLayer:" + bitListLayer);
                    }
                    count++;
                }
            }
            // uncompress remaining nodes
            list.addAll(uncompressRemainingNodes(layerID, remainingNodesLayer));
            //System.out.println("step z");
        }

        //System.out.println("uncompressReference : " + list.toString() + "id : " + id);
        return list;

    }

    /**
     * Returns the list of Ids of the children of the node, their parents and so on.
     * @param nodeID
     * @return
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public  SortedSet<Integer> findAllTheSuccessors(Integer nodeID) throws FileNotFoundException, UnsupportedEncodingException {
        SortedSet<Integer> successors = new TreeSet<Integer>();
        SortedSet<Integer> toUncompress = new TreeSet<Integer>();
        toUncompress.add(nodeID);
        while (!toUncompress.isEmpty()){
            Integer node = toUncompress.first();
            toUncompress.remove(node);
            SortedSet<Integer> uncompressed = uncompressAncestorsSuccessorsWithLayer(node, false, true).second().second();
            for (Integer successor : uncompressed) {
                if(!successors.contains(successor)) {
                    successors.add(successor);
                    toUncompress.add(successor);
                }
            }
        }
        uncompressAncestorsSuccessorsWithLayer(nodeID, true, false);
        return successors;
    }

    /**
     * Returns the list of Ids of the parents of the node, their parents and so on.
     * @param nodeID
     * @return
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public  SortedSet<Integer> findAllTheAncestry(Integer nodeID) throws FileNotFoundException, UnsupportedEncodingException {
        SortedSet<Integer> ancestry = new TreeSet<Integer>();
        SortedSet<Integer> toUncompress = new TreeSet<Integer>();
        toUncompress.add(nodeID);
        while (!toUncompress.isEmpty()){
            Integer node = toUncompress.first();
            toUncompress.remove(node);
            SortedSet<Integer> uncompressed = uncompressAncestorsSuccessorsWithLayer(node, true, false).first().second();
            for (Integer ancestor : uncompressed) {
                if(!ancestry.contains(ancestor)) {
                    ancestry.add(ancestor);
                    toUncompress.add(ancestor);
                }
            }
        }
        uncompressAncestorsSuccessorsWithLayer(nodeID, true, false);
        return ancestry;
    }

    /**
     * Encodes the annotation on edges and vertexes issued by SPADE using dictionary encoding and store it in hashmaps
     * @param textfile The data issued by SPADE
     * @return A pair of hashmaps.The first one contains the vertexes, the key is the id of the vertex and the value a byte array containing the encoded annotations.
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public  void dictionaryEncoding(String textfile) throws FileNotFoundException, UnsupportedEncodingException {
        //Deflater compresser = new Deflater(Deflater.BEST_COMPRESSION);
        File file = new File(textfile + ".txt");
        Scanner sc = new Scanner(file);
        String sep = "}{";
        byte[] separatorInput = sep.getBytes("UTF-8");
        byte [] separator = new byte[separatorInput.length];
        compresser.setInput(separatorInput);
        compresser.finish();
        compresser.deflate(separator);
        while (sc.hasNextLine()) {
            String toCompress = sc.nextLine();
            if (toCompress.substring(0, 4).equals("EDGE")) {
                String infoToCompress = toCompress.substring(toCompress.indexOf('{') + 1, toCompress.indexOf('}'));
                byte[] input = infoToCompress.getBytes("UTF-8");
                byte [] output = new byte[input.length + 100];
                compresser.setInput(input);
                compresser.finish();
                int compressedDataLength = compresser.deflate(output);
                putNext(toCompress.substring(toCompress.indexOf('(')+1, toCompress.indexOf(')')), output.toString(), "}{" );
                //	} else {
                //edges.putAnnotation(key, output);
                //}
                compresser.reset();
            }
            if (toCompress.substring(0, 4).equals("VERT")) {
                String infoToCompress = toCompress.substring(toCompress.indexOf('{') + 1, toCompress.indexOf('}'));
                byte [] input = infoToCompress.getBytes("UTF-8");
                byte [] output = new byte[input.length + 100];
                compresser.setInput(input);
                compresser.finish();
                int compressedDataLength = compresser.deflate(output);
                Integer node = Integer.parseInt(toCompress.substring(toCompress.indexOf('(')+1, toCompress.indexOf(")")));
				/*	DatabaseEntry key = new DatabaseEntry(node.toString().getBytes("UTF-8"));
			DatabaseEntry value = new DatabaseEntry(output);
			annotationsDatabase.putAnnotation(null, key, value);*/
                putAnnotation(node.toString(), output.toString());
                //System.out.println(compressedDataLength);
                compresser.reset();
            }
        }
        //compresser.end();
        sc.close();
        //Pair<HashMap< Integer, byte[]>, HashMap< String, byte[]>> maps = new Pair<HashMap< Integer, byte[]>, HashMap< String, byte[]>>(vertexes, edges);
        //return maps;
    }


    /**
     * getAnnotation the set of annotations of a vertex
     * @param toDecode node ID
     * @return the set of annotations as a String
     * @throws DataFormatException
     * @throws UnsupportedEncodingException
     */
    public  String decodingVertex(Integer toDecode) throws DataFormatException, UnsupportedEncodingException {
        Inflater decompresser = new Inflater();
        //byte[] input = encoded.first().getAnnotation(toDecode);
        String outputString = getAnnotation(toDecode.toString());

        return outputString;
    }


    /**
     * getAnnotation the set of annotations of an edge
     * @param node1 source vertex of the edge
     * @param node2 destination vertex of he edge
     * @return the set of annotations as a String
     * @throws DataFormatException
     * @throws UnsupportedEncodingException
     */
    public  String decodingEdge(Integer node1, Integer node2) throws DataFormatException, UnsupportedEncodingException {
        Inflater decompresser = new Inflater();
        String key_s = node1.toString() + "->" + node2.toString();
		/*DatabaseEntry key = new DatabaseEntry(key_s.getBytes("UTF-8"));
	DatabaseEntry data = new DatabaseEntry();
	annotationsDatabase.getAnnotation(null, key, data, LockMode.DEFAULT);*/
        String outputString = getAnnotation(key_s);

        return outputString;
    }


    /**
     * getAnnotation the Time annotation of an edge
     * @param node1 source vertex of the edge
     * @param node2 destination vertex of he edge
     * @return the time annotation as a String
     * @throws UnsupportedEncodingException
     * @throws DataFormatException
     */
    public String getTime(Integer node1, Integer node2) throws UnsupportedEncodingException, DataFormatException{
        String data = this.decodingEdge(node1, node2);
        String output = "";
        while(data.contains("time:")){
            data = data.substring(data.indexOf("time:")+5);
            output += data.substring(0, data.indexOf(',')) + " ";
        }
        return output;
    }


    /**
     * renames Nodes by giving them IDs instead of hashes and produces a TextFile exactly likt the one in entry but with IDs instead of hashes
     * @param textfile TestFile storage issued by SPADE
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public  void renameNodes(String textfile) throws FileNotFoundException, UnsupportedEncodingException{
        String output = textfile + "_preproc.txt";
        File file = new File(textfile + ".txt");
        Scanner sc = new Scanner(file );
        Integer nextID = 0;
        //Vector<String> alreadyRenamed = new Vector<String>();
        PrintWriter writer = new PrintWriter(output, "UTF-8");

        while(sc.hasNextLine()){
            String aux = sc.nextLine();
            if(aux.substring(0,6).equals("VERTEX")) {
                // set new id of the node aka. nextID and putAnnotation its hashcode in the vector then write the line in file.
                String hashID = aux.substring(8,72);
                alreadyRenamed.add(hashID);
                aux = aux.replaceFirst(hashID, nextID.toString());
                writer.println(aux);
                nextID++;
            }
            else if(aux.substring(0,4).equals("EDGE")) {
                // find in the vector the id corresponding to the two vertex involved and replace it in the line. Then write the line to file.
                String node1 = aux.substring(6, 70);
                String node2 = aux.substring(74, 138);
                Integer id1 = alreadyRenamed.indexOf(node1);
                Integer id2 = alreadyRenamed.indexOf(node2);
                aux = aux.replaceFirst(node1, id1.toString());
                aux = aux.replaceFirst(node2, id2.toString());
                writer.println(aux);
            }

        }
        sc.close();
        writer.close();
    }

    /**
     * update the scaffold database, taking a TextFile issued by SPADE as an entry
     * @param textfile
     * @return
     */
    public  boolean update(String textfile) {
        try {
            //renameNodes(textfile);
            HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> asl = createAncestorSuccessorList(textfile);
            updateAncestorsSuccessors(asl);
            dictionaryEncoding(textfile);
            return true;
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            return false;
        }
    }


    /**
     * Update the scaffold database
     * @param asl map issued by encodeAncestorsSuccessorsWithLayer
     */
    private  void updateAncestorsSuccessors(Map<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> asl) {
        try{
            //for each line to update
            Set<Map.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entries = asl.entrySet();
            SortedSet<Integer> ancestors;
            SortedSet<Integer> successors;
            HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> toUpdate = new HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>();
            //HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> unchangedToUpdate = new HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>();
            for (HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToUpdate : entries){
                Integer id = nodeToUpdate.getKey();
                Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> lists = uncompressAncestorsSuccessorsWithLayer(id, true, true);
                ancestors = lists.first().second();
                ancestors.addAll(nodeToUpdate.getValue().first());
                successors = lists.second().second();
                successors.addAll(nodeToUpdate.getValue().second());
                toUpdate.put(id, new Pair(ancestors, successors));

                //update other nodes
                for (Integer nodeID = id + 1; nodeID < id + W + 1; nodeID++) {
                    String line = getScaffold(nodeID.toString());
                    if (line != null && line.contains("/")) {
                        // getScaffold reference and see if it is id.
                        String ancestorList = line.substring(line.indexOf(' ')+1, line.indexOf("/") - 1);
                        boolean isReferenceAncestor;
                        if (ancestorList.contains("_")) {
                            isReferenceAncestor = false;
                        } else {
                            Integer referenceAncestor = Integer.parseInt(ancestorList.substring(0, ancestorList.indexOf(' ')));
                            isReferenceAncestor = id.equals(nodeID + referenceAncestor);
                        }
                        String successorList = line.substring(line.indexOf("/") + 2);
                        successorList = successorList.substring(successorList.indexOf(' ') + 1);
                        boolean isReferenceSuccessor;
                        if (successorList.contains("_")) {
                            isReferenceSuccessor = false;
                        } else {
                            Integer referenceSuccessor = Integer.parseInt(successorList.substring(0, successorList.indexOf(' ')));
                            isReferenceSuccessor = id.equals(nodeID + referenceSuccessor);
                        }
                        if ( isReferenceAncestor|| isReferenceSuccessor ){
                            //update the encoding of the line
                            Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> aux = uncompressAncestorsSuccessorsWithLayer(nodeID, true, true);
                            if (!toUpdate.containsKey(nodeID))
                                toUpdate.put(nodeID, new Pair(aux.first().second(), aux.second().second()));
                        }
                    }
                }
            }
            Set<HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entriesToUpdate = toUpdate.entrySet();
            for(HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToUpdate : entriesToUpdate) {
                //benchmarks.println("nodeToUpdate:" + nodeToUpdate);
                encodeAncestorsSuccessors(nodeToUpdate);
            }} catch (Exception ex) {
            System.out.println("update unsuccessful:" + ex.getMessage());
        }
    }


    @Override
    public AbstractEdge getEdge(String childVertexHash, String parentVertexHash) {return null;}


    @Override
    public AbstractVertex getVertex(String vertexHash) {return null;}



    @Override
    public Graph getChildren(String parentHash) {
        // TODO Auto-generated method stub

        return null;
    }


    @Override
    public Graph getParents(String childVertexHash) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public boolean putEdge(AbstractEdge incomingEdge) {
        try {
            String srcHash = incomingEdge.getChildVertex().bigHashCode();
            String dstHash = incomingEdge.getParentVertex().bigHashCode();
            Integer srcID = hashToID.get(srcHash);
            Integer dstID = hashToID.get(dstHash);
            long auxClock = System.nanoTime();
            StringBuilder annotationString = new StringBuilder();
            //annotationString.append("EDGE (" + srcId + " -> " + dstId + "): {");
            for (Map.Entry<String, String> currentEntry : incomingEdge.getAnnotations().entrySet()) {
                String key = currentEntry.getKey();
                String value = currentEntry.getValue();
                if (key == null || value == null) {
                    continue;
                }
                annotationString.append(key);
                annotationString.append(":");
                annotationString.append(value);
                annotationString.append(",");
            }
            //annotationString.append("}\n");
            String edgeString = annotationString.toString();
            //outputFile.write(edgeString);
            byte [] input = edgeString.getBytes("UTF-8");
            byte [] output = new byte[input.length + 100];
            compresser.setInput(input);
            compresser.finish();
            int compressedDataLength = compresser.deflate(output);
            String key = srcID + "->" + dstID;
            putAnnotation(key, output.toString());
            compresser.reset();
            long auxClock2 = System.nanoTime();
            clockAnnotations = clockAnnotations + auxClock2-auxClock;
            // scaffold storage
            //update scaffoldInMemory
            Pair<SortedSet<Integer>, SortedSet<Integer>> srcLists = scaffoldInMemory.get(srcID);
            if (srcLists == null) {
                srcLists = new Pair<SortedSet<Integer>, SortedSet<Integer>>(new TreeSet<Integer>(), new TreeSet<Integer>());
            }
            srcLists.second().add(dstID);
            scaffoldInMemory.put(srcID, srcLists);
            Pair<SortedSet<Integer>, SortedSet<Integer>> dstLists = scaffoldInMemory.get(dstID);
            if (dstLists == null) {
                dstLists = new Pair<SortedSet<Integer>, SortedSet<Integer>>(new TreeSet<Integer>(), new TreeSet<Integer>());
            }
            dstLists.first().add(dstID);
            scaffoldInMemory.put(dstID, dstLists);
            edgesInMemory++;
            long auxClock3 = System.nanoTime();
            clockScaffold = clockScaffold + auxClock3 - auxClock2;
            if(edgesInMemory % maxEdgesInMemory == 0) {
                countUpdates ++;
                benchmarks.println("Time to putAnnotation 10000 edges in annotationsStorage (ns): " + clockAnnotations );
                annotationsTime += clockAnnotations;
                clockAnnotations = 0;
                long auxClock4 = System.nanoTime();
                updateAncestorsSuccessors(scaffoldInMemory);
                scaffoldInMemory.clear();
                long auxClock5 = System.nanoTime();
                clockScaffold = clockScaffold + auxClock5 - auxClock4;
                benchmarks.println("Time to putAnnotation 10000 edges in scaffoldStorage (ns): " + clockScaffold);
                scaffoldTime += clockScaffold;
                clockScaffold = 0;
            }

            return true;
        } catch (Exception exception) {
            Logger.getLogger(CompressedStorage.class.getName()).log(Level.SEVERE, null, exception);
            return false;
        }
    }


    @Override
    public boolean putVertex(AbstractVertex incomingVertex) {
        try {
            String vertexHash = incomingVertex.bigHashCode();
            Integer vertexID = nextVertexID;
            nextVertexID ++;
            hashToID.put(vertexHash, vertexID);
            idToHash.put(vertexID, vertexHash);
            StringBuilder annotationString = new StringBuilder();
            //annotationString.append("VERTEX (" + vertexId + "): {");
            for (Map.Entry<String, String> currentEntry : incomingVertex.getAnnotations().entrySet()) {
                String key = currentEntry.getKey();
                String value = currentEntry.getValue();
                if (key == null || value == null) {
                    continue;
                }
                annotationString.append(key);
                annotationString.append(":");
                annotationString.append(value);
                annotationString.append(",");
            }
            //annotationString.append("}\n");
            String vertexString = annotationString.toString();
            //outputFile.write(vertexString);
            byte [] input = vertexString.getBytes("UTF-8");
            byte [] output = new byte[input.length + 100];
            compresser.setInput(input);
            compresser.finish();
            int compressedDataLength = compresser.deflate(output);
            putAnnotation(vertexID.toString(), output.toString());
            compresser.reset();
            return true;
        } catch (Exception exception) {
            Logger.getLogger(CompressedStorage.class.getName()).log(Level.SEVERE, null, exception);
            return false;
        }
    }


    @Override
    public Object executeQuery(String query) {
        // TODO Auto-generated method stub
        return null;
    }
    /**
     * getAnnotation the lineage of a vertex choosing the depth and the direction, like getLineage in spade.querry.scaffold
     * @param hash
     * @param direction
     * @param maxDepth
     * @return
     */
    public Map<String, Set<String>> getLineageMap(String hash, String direction, int maxDepth)
    {
        Integer id = hashToID.get(hash);
        System.out.println("ID = " + id);
        if(id != null)
        {
            Set<Integer> remainingVertices = new HashSet<>();
            Set<Integer> visitedVertices = new HashSet<>();
            Map<String, Set<String>> lineageMap = new HashMap<>();
            remainingVertices.add(id);
            int current_depth = 0;
            while(!remainingVertices.isEmpty() && current_depth < maxDepth)
            {
                visitedVertices.addAll(remainingVertices);
                Set<Integer> currentSet = new HashSet<>();
                for(Integer current_id: remainingVertices)
                {
                    Set<Integer> neighbors = null;
                    if(DIRECTION_ANCESTORS.startsWith(direction.toLowerCase())) {
                        Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> lists = uncompressAncestorsSuccessorsWithLayer(current_id, true, true);
                        if (lists == null) {
                            neighbors = null;
                        } else {
                            neighbors = lists.first().second();
                            //System.out.println("neighbors size:" + neighbors.size());
                        }
                    }
                    else if(DIRECTION_DESCENDANTS.startsWith(direction.toLowerCase())) {
                        //System.out.println("ok");
                        Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> lists = uncompressAncestorsSuccessorsWithLayer(current_id, true, true);
                        if (lists == null) {
                            neighbors = null;
                        } else {
                            neighbors = lists.second().second();
                            //System.out.println("neighbors size:" + neighbors.size() + " - " + neighbors.toString());
                        }
                    }
                    if(neighbors != null)
                    {
                        //System.out.println(neighbors.size());
                        String current_hash = idToHash.get(current_id);
                        Set<String> neighbors_hash = new TreeSet<String>();
                        for(Integer vertexId: neighbors)
                        {
                            neighbors_hash.add(idToHash.get(vertexId));
                            if(!visitedVertices.contains(vertexId))
                            {
                                //System.out.println("true");
                                currentSet.addAll(neighbors);
                            }
                        }
                        lineageMap.put(current_hash, neighbors_hash);
                        //System.out.println(current_hash + " - " + neighbors_hash.toString());
                    }
                }
                remainingVertices.clear();
                remainingVertices.addAll(currentSet);
                current_depth++;
            }

            return lineageMap;
        }

        return null;
    }


}
