package spade.storage;


import com.mysql.jdbc.StringUtils;
import com.sleepycat.je.utilint.Pair;
import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Edge;
import spade.core.Graph;
import spade.core.Vertex;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

public abstract class CompressedStorage extends AbstractStorage
{
    protected static Integer nextVertexID;
    protected static Integer edgeCount = 0;
    public static Integer W;
    public static Integer L;
    protected static Deflater deflater;
    protected static Vector<String> alreadyRenamed;
    protected static Map<String, Integer> hashToID;
    protected static Map<Integer, String> idToHash;
    protected static int edgesInMemory;
    protected static Map<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> uncompressedBuffer;
    protected static final Logger logger = Logger.getLogger(CompressedStorage.class.getName());
    protected static int maxEdgesInMemory = 10;    // assign to 1 for ignoring uncompressedBuffer
    protected static boolean useDeltaForRunLength = true;    // assign to false for using node ids in run length encoding
    protected static boolean optimizeReferenceSelection = true;    // assign to false for normal reference selection
    protected boolean useAdjacencyListCache = true;   // assign to false to avoid building and using adjacencyListCache
    public Map<String, String> adjacencyListCache;
    protected final int GLOBAL_TX_SIZE = 10000;
    public static PrintWriter filePrinter;
    public static int adjacencyListCacheHits = 0;
    public static int getScaffolds = 0;
    private Map<String, AbstractVertex> queryVertices = new HashMap<>();
    private int resetCounter = 0;
    private static final int RESET_COUNT = 1000000000;

    /**
     * This method is invoked by the kernel to initialize the storage.
     *
     * @param filePath The arguments with which this storage is to be
     *                  initialized.
     * @return True if the storage was initialized successfully.
     */
    public boolean initialize(String filePath)
    {
        deflater = new Deflater(Deflater.BEST_COMPRESSION);
        W=10;
        L=5;
        nextVertexID = 0;
        hashToID = new HashMap<>();
        idToHash = new HashMap<>();
        alreadyRenamed = new Vector<>();
        uncompressedBuffer = new HashMap<>();
        adjacencyListCache = new HashMap<>();
        edgesInMemory = 0;

        return true;
    }

    public int vertexCount()
    {
        return nextVertexID;
    }

    public int edgeCount()
    {
        return edgeCount;
    }


    /**
     * This method is invoked by the kernel to shut down the storage.
     *
     * @return True if the storage was shut down successfully.
     */
    public abstract boolean shutdown();

    public boolean encodeAncestorsSuccessors(HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToCompress)
    {
        //find reference node
        Integer id = nodeToCompress.getKey();
        SortedSet<Integer> ancestors = nodeToCompress.getValue().first();
        SortedSet<Integer> successors = nodeToCompress.getValue().second();
        //first integer is the max of nodes in common, the second one is the number of 0 in the corresponding bit list
        Pair<Integer, Integer> maxNodesInCommonAncestor = new Pair<>(0,0);
        Pair<Integer, Integer> maxNodesInCommonSuccessor = new Pair<>(0,0);
        String bitlistAncestor = "";
        int layerAncestor = 1;
        Integer referenceAncestor = -1;
        SortedSet<Integer> referenceAncestorList = new TreeSet<>();
        String bitlistSuccessor = "";
        int layerSuccessor = 1;
        Integer referenceSuccessor = -1;
        SortedSet<Integer> referenceSuccessorList = new TreeSet<>();
        for (Integer possibleReferenceID = Math.max(0, id - W); possibleReferenceID < id; possibleReferenceID++)
        {
            //for each node in the W last nodes seen, compute the proximity,
            // i.e. the number of successors of the current node that also are successors of the possibleReference node.
            Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> asl =
                    uncompressAncestorsSuccessorsWithLayer(possibleReferenceID, true, true);
            if(asl.first().first() < L)
            {
                // nodesInCommonAncestor -> < <nodesInCommon, numberOfZero>, bitlist>
                Pair<Pair<Integer, Integer>, String> nodesInCommonAncestor = commonNodes(asl.first().second(), ancestors);
                int numberOfOneAncestor = nodesInCommonAncestor.first().first();
                int numberOfZeroAncestor = nodesInCommonAncestor.first().second();
                int maxNumberOfOneAncestor = maxNodesInCommonAncestor.first();
                int maxNumberOfZeroAncestor = maxNodesInCommonAncestor.second();
                boolean selectReference;
                if(optimizeReferenceSelection)
                {
                    selectReference =
                            (numberOfOneAncestor > maxNumberOfOneAncestor) || (numberOfOneAncestor == maxNumberOfOneAncestor
                                    && numberOfZeroAncestor < maxNumberOfZeroAncestor);
                }
                else
                {
                    selectReference = (numberOfOneAncestor > maxNumberOfOneAncestor);
                }
                if (selectReference)
                {
                    maxNodesInCommonAncestor = nodesInCommonAncestor.first();
                    bitlistAncestor = nodesInCommonAncestor.second();
                    referenceAncestor = possibleReferenceID;
                    layerAncestor = asl.first().first() + 1;
                    referenceAncestorList = asl.first().second();
                }
            }
            if (asl.second().first() < L)
            {
                Pair<Pair<Integer, Integer>, String> nodesInCommonSuccessor =  commonNodes(asl.second().second(), successors);
                int numberOfOneSuccessor = nodesInCommonSuccessor.first().first();
                int numberOfZeroSuccessor = nodesInCommonSuccessor.first().second();
                int maxNumberOfOneSuccessor = maxNodesInCommonSuccessor.first();
                int maxNumberOfZeroSuccessor = maxNodesInCommonSuccessor.second();
                boolean selectReference;
                if(optimizeReferenceSelection)
                {
                    selectReference = numberOfOneSuccessor > maxNumberOfOneSuccessor || (numberOfOneSuccessor == maxNumberOfOneSuccessor
                            && numberOfZeroSuccessor < maxNumberOfZeroSuccessor);
                }
                else
                {
                    selectReference = (numberOfOneSuccessor > maxNumberOfOneSuccessor);
                }
                if (selectReference)
                {
                    maxNodesInCommonSuccessor = nodesInCommonSuccessor.first();
                    bitlistSuccessor = nodesInCommonSuccessor.second();
                    referenceSuccessor = possibleReferenceID;
                    layerSuccessor = asl.second().first() + 1;
                    referenceSuccessorList = asl.second().second();
                }
            }
        }

        //encode ancestor list
        SortedSet<Integer> remainingNodesAncestor = new TreeSet<>();
        remainingNodesAncestor.addAll(ancestors);
        //encode reference
        String encoding = layerAncestor + " ";
        if (maxNodesInCommonAncestor.first() > 0)
        {
            encoding = encoding + (referenceAncestor - id) + " " + bitlistAncestor + "";
            //keep only remaining nodes
            remainingNodesAncestor.removeAll(referenceAncestorList);
        }
        else
        {
            encoding = encoding + "_";
        }

        //encode consecutive nodes and delta encoding
        Integer previousNodeID = id;
        int countConsecutives = 0;
        for (Integer nodeID : remainingNodesAncestor)
        {
            Integer delta = nodeID - previousNodeID;
            if (delta == 1)
            {
                countConsecutives++;
            }
            else
            {
                if (countConsecutives > 0)
                {
                    if(!useDeltaForRunLength)
                    {
                        int idx = encoding.lastIndexOf(" ");
                        String encoding_last = encoding.substring(0, idx + 1);
                        encoding = encoding_last + (previousNodeID - countConsecutives);
                    }
                    encoding = encoding + ":" + countConsecutives;
                }
                encoding = encoding + " " + delta;
                countConsecutives = 0;
            }
            previousNodeID = nodeID;
        }
        // encode successor list
        SortedSet<Integer> remainingNodesSuccessor = new TreeSet<>();
        remainingNodesSuccessor.addAll(successors);
        //encode reference
        encoding = encoding + " / " + layerSuccessor + " ";
        if (maxNodesInCommonSuccessor.first() > 0)
        {
            encoding = encoding + (referenceSuccessor - id) + " " + bitlistSuccessor + "";
            //keep only remaining nodes
            remainingNodesSuccessor.removeAll(referenceSuccessorList);
        }
        else
        {
            encoding = encoding + "_ ";
        }

        //encode consecutive nodes and delta encoding
        previousNodeID = id;
        countConsecutives = 0;
        for (Integer nodeID : remainingNodesSuccessor)
        {
            Integer delta = nodeID - previousNodeID;
            if (delta == 1)
            {
                countConsecutives++;
            }
            else
            {
                if (countConsecutives > 0)
                {
                    if(!useDeltaForRunLength)
                    {
                        int idx = encoding.lastIndexOf(" ");
                        String encoding_last = encoding.substring(0, idx + 1);
                        encoding = encoding_last + (previousNodeID - countConsecutives);
                    }
                    encoding = encoding + ":" + countConsecutives;
                }
                encoding = encoding + " " + delta;
                countConsecutives = 0;
            }
            previousNodeID = nodeID;

        }
        putScaffoldEntry(id.toString(), encoding);
        return true;
    }

    public Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> uncompressAncestorsSuccessorsWithLayer(
            Integer id, boolean uncompressAncestors, boolean uncompressSuccessors)
    {
        SortedSet<Integer> ancestors = new TreeSet<>();
        SortedSet<Integer> successors = new TreeSet<>();
        Integer ancestorLayer = 1;
        Integer successorLayer = 1;
        String scaffoldEntry = (String) getScaffoldEntry(id.toString());

        if(scaffoldEntry != null && scaffoldEntry.contains("/"))
        {
            // split the line in two parts : ancestor list and successor list.
            String ancestorList = scaffoldEntry.substring(0, scaffoldEntry.indexOf('/'));
            ancestorLayer = Integer.parseInt(ancestorList.substring(0, ancestorList.indexOf(' ')));
            ancestorList = ancestorList.substring(ancestorList.indexOf(' ') + 1);
            //uncompressAncestors
            if (uncompressAncestors)
            {
                if(ancestorList.contains("_"))
                { // means there is no reference
                    String ancestorList2 = ancestorList.substring(ancestorList.indexOf("_") + 1);
                    ancestors.addAll(uncompressRemainingNodes(id, ancestorList2));
                }
                else
                { // there is a reference that we have to uncompress
                    // uncompress the remaining Nodes
                    String remaining = ancestorList.substring(ancestorList.indexOf(" ") + 1);
                    remaining = remaining.substring(remaining.indexOf(" ")+1);
                    ancestors.addAll(uncompressRemainingNodes(id, remaining));
                    //uncompress the reference and its reference after that
                    try
                    {
                        ancestors.addAll(uncompressReference(id, ancestorList, true));
                    }
                    catch (Exception ex)
                    {
                        logger.log(Level.SEVERE, null, ex);
                    }
                }
            }
            String successorList = scaffoldEntry.substring(scaffoldEntry.indexOf('/') + 2);
            successorLayer = Integer.parseInt(successorList.substring(0, successorList.indexOf(' ')));
            successorList = successorList.substring(successorList.indexOf(' ') + 1);
            // uncompressSuccessors
            if (uncompressSuccessors)
            {
                // means there is no reference
                if(successorList.contains("_"))
                {
                    String successorList2 = successorList.substring(successorList.indexOf("_")+ 1);
                    successors.addAll(uncompressRemainingNodes(id, successorList2));
                }
                else
                { // there is a reference that we have to uncompress
                    // uncompress the remaining Nodes
					/*			String remaining = successorList.substring(successorList.indexOf(" ")+1);
					remaining = remaining.substring(remaining.indexOf(" ")+1);
					System.out.println("remaining" +remaining);
					//System.out.println("step i ");
					successors.addAll(uncompressRemainingNodes(id, remaining));*/
                    //uncompress the reference and its reference after that
                    try
                    {
                        successors.addAll(uncompressReference(id, successorList, false));
                    }
                    catch (Exception ex)
                    {
                        logger.log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        Pair<Integer, SortedSet<Integer>> ancestorsAndLayer = new Pair<>(ancestorLayer, ancestors);
        Pair<Integer, SortedSet<Integer>> successorsAndLayer = new Pair<>(successorLayer, successors);
        return new Pair<>(ancestorsAndLayer, successorsAndLayer);
    }

    public Pair<Pair<Integer, Integer>, String> commonNodes(SortedSet<Integer> reference,
                                                                   SortedSet<Integer> node)
    {
        int nodesInCommon = 0;
        int numberOfZero = 0;
        String bitlist = "";
        for (Integer successor : reference)
        {
            if (node.contains(successor))
            {
                nodesInCommon++;
                bitlist += "1";
            }
            else
            {
                numberOfZero++;
                bitlist += "0";
            }
        }
        Pair<Integer, Integer> count = new Pair<>(nodesInCommon, numberOfZero);

        return new Pair<>(count, bitlist);
    }

    public abstract boolean putAnnotationEntry(String key, byte[] value);

    public abstract byte[] getAnnotationEntry(String key);

    public abstract boolean putScaffoldEntry(String key, String value);

    public abstract Object getScaffoldEntry(String key);

    public SortedSet<Integer> uncompressRemainingNodes(Integer nodeID, String remainingNodes)
    {
        SortedSet<Integer> successors = new TreeSet<>();
        Integer currentID = nodeID;
        String uncompressing;
        int length;
        StringTokenizer st = new StringTokenizer(remainingNodes);
        while (st.hasMoreTokens())
        {
            uncompressing = st.nextToken();
            //uncompress consecutive numbers
            if(uncompressing.contains(":"))
            {
                if(uncompressing.charAt(0) != ':')
                {
                    if(useDeltaForRunLength)
                    {
                        currentID += Integer.parseInt(uncompressing.substring(0, uncompressing.indexOf(':')));
                    }
                    else
                    {
                        currentID = Integer.parseInt(uncompressing.substring(0, uncompressing.indexOf(':')));
                    }
                    successors.add(currentID);
                }
                length = Integer.parseInt(uncompressing.substring(uncompressing.indexOf(':') + 1));
                for (int i = 0; i < length; i++)
                {
                    currentID++;
                    successors.add(currentID);
                }
            }
            else
            {
                currentID = currentID + Integer.parseInt(uncompressing);
                successors.add(currentID);
            }
        }
        return successors;
    }

    private SortedSet<Integer> uncompressReference(Integer id, String ancestorOrSuccessorList,
                                                          boolean ancestorOrSuccessor)
    {

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
        while (hasReference)
        {
            long storageClock = System.nanoTime();
            String currentLine = (String) getScaffoldEntry(referenceID.toString());

            if(currentLine.length() > 0)
            {

                // we want to uncompress ancestors
                if (ancestorOrSuccessor)
                {
                    toUncompress = currentLine.substring(currentLine.indexOf(' ')+1, currentLine.indexOf("/") - 1);
                }
                else
                { // we want to uncompress successors
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
                //filePrinter.println("____ " + layer + " /// " + remainingNodesLayer);
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
                //filePrinter.println("ref:" + layer + " ////remaining:" + remainingNodesLayer + "////bitListLayer:" + bitListLayer );
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
     */
    public SortedSet<Integer> findAllTheSuccessors(Integer nodeID)
    {
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
     */
    public SortedSet<Integer> findAllTheAncestry(Integer nodeID)
    {
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
     * Update the scaffold database
     */
    void updateAncestorsSuccessors()
    {
        try
        {
            //for each line to update
            Set<Map.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entries =
                    CompressedStorage.uncompressedBuffer.entrySet();
            SortedSet<Integer> ancestors;
            SortedSet<Integer> successors;
            HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> toUpdate = new HashMap<>();
            for (HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToUpdate : entries)
            {
                Integer id = nodeToUpdate.getKey();
                Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> lists =
                        uncompressAncestorsSuccessorsWithLayer(id, true, true);
                ancestors = lists.first().second();
                ancestors.addAll(nodeToUpdate.getValue().first());
                successors = lists.second().second();
                successors.addAll(nodeToUpdate.getValue().second());
                toUpdate.put(id, new Pair<>(ancestors, successors));

                //update other nodes
                for (Integer nodeID = id + 1; nodeID < id + W + 1; nodeID++)
                {
                    String line = (String) getScaffoldEntry(nodeID.toString());
                    if (line != null && line.contains("/"))
                    {
                        // getAnnotation reference and see if it is id.
                        String ancestorList = line.substring(line.indexOf(' ') + 1, line.indexOf("/") - 1);
                        boolean isReferenceAncestor;
                        if (ancestorList.contains("_"))
                        {
                            isReferenceAncestor = false;
                        }
                        else
                        {
                            Integer referenceAncestor = Integer.parseInt(ancestorList.substring(0, ancestorList.indexOf(' ')));
                            isReferenceAncestor = id.equals(nodeID + referenceAncestor);
                        }
                        String successorList = line.substring(line.indexOf("/") + 2);
                        successorList = successorList.substring(successorList.indexOf(' ') + 1);
                        boolean isReferenceSuccessor;
                        if (successorList.contains("_"))
                        {
                            isReferenceSuccessor = false;
                        }
                        else
                        {
                            Integer referenceSuccessor = Integer.parseInt(successorList.substring(0, successorList.indexOf(' ')));
                            isReferenceSuccessor = id.equals(nodeID + referenceSuccessor);
                        }
                        if ( isReferenceAncestor|| isReferenceSuccessor )
                        {
                            //update the encoding of the line
                            Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> aux =
                                    uncompressAncestorsSuccessorsWithLayer(nodeID, true, true);
                            if (!toUpdate.containsKey(nodeID))
                                toUpdate.put(nodeID, new Pair<>(aux.first().second(), aux.second().second()));
                        }
                    }
                }
            }
            for(HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToUpdate : toUpdate.entrySet())
            {
                encodeAncestorsSuccessors(nodeToUpdate);
            }
        }
        catch (Exception ex)
        {
            System.out.println("update unsuccessful: " + ex.getMessage());
        }
    }

    public AbstractVertex getVertex(String vertexHash)
    {
        AbstractVertex vertex = new Vertex();
        Integer id = hashToID.get(vertexHash);
        String vertexString = getVertex(id);
        String[] splits = vertexString.split(",");
        for(String split: splits)
        {
            String temp_string = split.trim();
            int colon_index = temp_string.indexOf(":");
            if(colon_index != -1)
            {
                String key = temp_string.substring(0, colon_index).trim();
                String value = temp_string.substring(colon_index+1).trim();
                if(StringUtils.isNullOrEmpty(key))
                    continue;
                vertex.addAnnotation(key, value);
            }
        }

        return vertex;
    }

    public String getVertex(Integer vertexID)
    {
        try
        {
            Inflater decompresser = new Inflater();
            byte[] input = getAnnotationEntry(vertexID.toString());
            String vertexString;
            if(input.length == 0)
            {
                vertexString = "Vertex " + vertexID + " does not exist.";
            }
            else
            {
                decompresser.setInput(input);
                byte[] result = new byte[1000];
                int resultLength = decompresser.inflate(result);
                decompresser.end();

                // Decode the bytes into a String
                vertexString = new String(result, 0, resultLength, "UTF-8");
            }
            return vertexString;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error getting and decoding Vertex", ex);
        }
        return null;
    }

    @Override
    public Graph getChildren(String parentHash)
    {
        return null;
    }

    @Override
    public Graph getParents(String childVertexHash)
    {
        return null;
    }

    public AbstractEdge getEdge(String childVertexHash, String parentVertexHash)
    {
        Integer childID = hashToID.get(childVertexHash);
        Integer parentID = hashToID.get(parentVertexHash);
        AbstractVertex childVertex = queryVertices.get(childVertexHash);
        AbstractVertex parentVertex = queryVertices.get(parentVertexHash);
        AbstractEdge edge = new Edge(childVertex, parentVertex);

        String edgeString = getEdge(childID, parentID);
        String[] splits = edgeString.split(",");
        for(String split: splits)
        {
            String temp_string = split.trim();
            int colon_index = temp_string.indexOf(":");
            if(colon_index != -1)
            {
                String key = temp_string.substring(0, colon_index).trim();
                String value = temp_string.substring(colon_index+1).trim();
                if(StringUtils.isNullOrEmpty(key))
                    continue;
                edge.addAnnotation(key, value);
            }
        }

        return edge;
    }

    public String getEdge(Integer childID, Integer parentID)
    {
        try
        {
            String edgeID = childID + "->" + parentID; //TODO: change key to edgeHash
            Inflater decompresser = new Inflater();
            byte[] input = getAnnotationEntry(edgeID);
            String edgeString;
            if(input == null || input.length == 0)
            {
                edgeString = "Edge " + edgeID + " does not exist.";
            }
            else
            {
                decompresser.setInput(input);
                byte[] result = new byte[1000];
                int resultLength = decompresser.inflate(result);
                decompresser.end();

                // Decode the bytes into a String
                edgeString = new String(result, 0, resultLength, "UTF-8");
            }
            return edgeString;
        }
        catch(Exception ex)
        {
            logger.log(Level.SEVERE, "Error getting and decoding Edge", ex);
        }
        return null;
    }


    public boolean putEdge(AbstractEdge incomingEdge)
    {
        try
        {
            // compress annotation and put in storage
            String childHash = incomingEdge.getChildVertex().bigHashCode();
            String parentHash = incomingEdge.getParentVertex().bigHashCode();
            Integer childID = hashToID.get(childHash);
            Integer parentID = hashToID.get(parentHash);
            StringBuilder edgeStringBuilder = new StringBuilder();
            for (Map.Entry<String, String> currentEntry : incomingEdge.getAnnotations().entrySet())
            {
                String key = currentEntry.getKey();
                String value = currentEntry.getValue();
                if (StringUtils.isNullOrEmpty(key))
                {
                    continue;
                }
                if(value == null)
                    value = "";
                edgeStringBuilder.append(key);
                edgeStringBuilder.append(":");
                edgeStringBuilder.append(value);
                edgeStringBuilder.append(",");
            }
            String edgeString = edgeStringBuilder.substring(0, edgeStringBuilder.length() - 1);
            byte [] input = edgeString.getBytes(StandardCharsets.UTF_8);
            byte [] temp = new byte[input.length];
            deflater.setInput(input);
            deflater.finish();
            int output_size = deflater.deflate(temp);
            byte[] output = Arrays.copyOf(temp, output_size);
            String key = childID + "->" + parentID; //TODO: change key to edgeHash
            putAnnotationEntry(key, output);
            resetCounter++;
            if((resetCounter % RESET_COUNT) == 0)
            {
                deflater.reset();
            }

            // compress scaffold and put in storage
            // update uncompressedBuffer here
            Pair<SortedSet<Integer>, SortedSet<Integer>> childLists = uncompressedBuffer.get(childID);
            if (childLists == null)
            {
                childLists = new Pair<>(new TreeSet<>(), new TreeSet<>());
            }
            childLists.second().add(parentID);
            uncompressedBuffer.put(childID, childLists);
            Pair<SortedSet<Integer>, SortedSet<Integer>> parentLists = uncompressedBuffer.get(parentID);
            if (parentLists == null)
            {
                parentLists = new Pair<>(new TreeSet<>(), new TreeSet<>());
            }
            parentLists.first().add(parentID);
            uncompressedBuffer.put(parentID, parentLists);
            edgesInMemory++;
            if(edgesInMemory % maxEdgesInMemory == 0)
            {
                updateAncestorsSuccessors();
                uncompressedBuffer.clear();
            }

            edgeCount++;
            return true;
        }
        catch (Exception exception)
        {
            logger.log(Level.SEVERE, null, exception);
            return false;
        }
    }

    @Override
    public boolean putVertex(AbstractVertex incomingVertex)
    {
        try
        {
            String vertexHash = incomingVertex.bigHashCode();
            Integer vertexID = nextVertexID;
            nextVertexID++;
            hashToID.put(vertexHash, vertexID);
            idToHash.put(vertexID, vertexHash);
            StringBuilder vertexStringBuilder = new StringBuilder();
            for (Map.Entry<String, String> currentEntry : incomingVertex.getAnnotations().entrySet())
            {
                String key = currentEntry.getKey();
                String value = currentEntry.getValue();
                if (StringUtils.isNullOrEmpty(key))
                {
                    continue;
                }
                if(value == null)
                    value = "";
                vertexStringBuilder.append(key);
                vertexStringBuilder.append(":");
                vertexStringBuilder.append(value);
                vertexStringBuilder.append(",");
            }
            String vertexString = vertexStringBuilder.substring(0, vertexStringBuilder.length() - 1);
            byte [] input = vertexString.getBytes(StandardCharsets.UTF_8);
//            deflater.setInput(input);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(input.length);
            DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(outputStream);
            deflaterOutputStream.write(input);
            deflaterOutputStream.close();
//            byte[] buffer = new byte[1024];
//            while (!deflater.finished())
//            {
//                int buffer_length = deflater.deflate(buffer);
//                outputStream.write(buffer, 0, buffer_length);
//            }
//            outputStream.close();
            byte[] output = outputStream.toByteArray();
            putAnnotationEntry(vertexID.toString(), output);
            resetCounter++;
            if((resetCounter % RESET_COUNT) == 0)
            {
                deflater.reset();
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.log(Level.SEVERE, null, ex);
            return false;
        }
    }


    @Override
    public abstract Object executeQuery(String query);

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
                    Pair<Pair<Integer, SortedSet<Integer>>, Pair<Integer, SortedSet<Integer>>> lists = uncompressAncestorsSuccessorsWithLayer(current_id, true, true);
                    if(lists == null)
                    {
                        neighbors = null;
                    }
                    else
                    {
                        if(DIRECTION_ANCESTORS.startsWith(direction.toLowerCase()))
                        {
                            neighbors = lists.first().second();
                        }
                        else if(DIRECTION_DESCENDANTS.startsWith(direction.toLowerCase()))
                        {
                            neighbors = lists.second().second();
                        }
                    }
                    String current_hash = idToHash.get(current_id);
                    Set<String> neighbors_hash = new TreeSet<>();
                    if(neighbors != null)
                    {
                        for(Integer vertexId: neighbors)
                        {
                            neighbors_hash.add(idToHash.get(vertexId));
                            if(!visitedVertices.contains(vertexId))
                            {
                                currentSet.addAll(neighbors);
                            }
                        }
                    }
                    lineageMap.put(current_hash, neighbors_hash);
                }
                remainingVertices.clear();
                remainingVertices.addAll(currentSet);
                current_depth++;
            }
            return lineageMap;
        }
        return null;
    }

    public Graph constructGraphFromLineageMap(Map<String, Set<String>> lineageMap, String direction)
    {
        Graph result = new Graph();
        for(Map.Entry<String, Set<String>> entry: lineageMap.entrySet())
        {
            String vertexHash = entry.getKey();
            AbstractVertex vertex = getVertex(vertexHash);
            if(vertex != null)
            {
                queryVertices.put(vertexHash, vertex);
                result.putVertex(vertex);
            }
            Set<String> neighborSet = entry.getValue();
            for(String neighborHash: neighborSet)
            {
                AbstractVertex neighborVertex = getVertex(neighborHash);
                if(neighborVertex != null)
                {
                    queryVertices.put(neighborHash, neighborVertex);
                    result.putVertex(neighborVertex);
                    AbstractEdge edge = null;
                    if(DIRECTION_DESCENDANTS.startsWith(direction.toLowerCase()))
                    {
                        edge = getEdge(vertexHash, neighborHash);
                    }
                    else if(DIRECTION_ANCESTORS.startsWith(direction.toLowerCase()))
                    {
                        edge = getEdge(neighborHash, vertexHash);
                    }
                    if(edge != null)
                    {
                        result.putEdge(edge);
                    }
                }
            }
        }

        return result;
    }
}
