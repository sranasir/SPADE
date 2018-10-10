package spade.storage;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.utilint.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class CompressedBerkeleyDB extends CompressedStorage
{
	private static Environment DatabaseEnvironment1 = null;
	private static Environment DatabaseEnvironment2 = null;
	private static Database scaffoldDatabase = null;
	private static Database annotationsDatabase = null;

	/**
	 * This method is invoked by the kernel to initialize the storage.
	 *
	 * @param filePath The arguments with which this storage is to be
	 *                  initialized.
	 * @return True if the storage was initialized successfully.
	 */
	public boolean initialize(String filePath)
	{
		try
		{
			super.initialize(filePath);
			benchmarks = new PrintWriter("benchmarks/compression_time_berkeleyDB.txt", "UTF-8");
			// Open the environment. Create it if it does not already exist.
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			File scaffoldFile = new File(filePath + "/scaffold");
			scaffoldFile.mkdirs();
			DatabaseEnvironment1 = new Environment(scaffoldFile,
					envConfig);
			File annotationsFile = new File(filePath + "/annotations");
			annotationsFile.mkdirs();
			DatabaseEnvironment2 = new Environment(annotationsFile,
					envConfig);
			// Open the databases. Create it if it does not already exist.
			DatabaseConfig DatabaseConfig1 = new DatabaseConfig();
			DatabaseConfig1.setAllowCreate(true);
			scaffoldDatabase = DatabaseEnvironment1.openDatabase(null, "spade_scaffold", DatabaseConfig1); 
			annotationsDatabase = DatabaseEnvironment2.openDatabase(null, "spade_annotations", DatabaseConfig1); 

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
			if (scaffoldDatabase != null)
				scaffoldDatabase.close();
			if (annotationsDatabase != null)
				annotationsDatabase.close();
			if (DatabaseEnvironment1 != null)
				DatabaseEnvironment1.close();
			if (DatabaseEnvironment2 != null)
				DatabaseEnvironment2.close();
			compressor.end();
			benchmarks.close();
			return true;
		}
		catch(DatabaseException ex)
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
	public HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> createAncestorSuccessorList( String textfile)
	{
		try
		{
			File file = new File(textfile + ".txt");
			Scanner sc = new Scanner(file);
			HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> ancestorsSuccessors = new HashMap<>();
			while(sc.hasNextLine())
			{
				String aux = sc.nextLine();
				if(aux.substring(0, 4).equals("EDGE"))
				{
					String node_s = aux.substring(6, aux.indexOf(" ->"));
					Integer node = Integer.parseInt(node_s);
					String successor_s = aux.substring(aux.indexOf(" -> ") + 4, aux.indexOf("): {"));
					Integer successor = Integer.parseInt(successor_s);
					if(ancestorsSuccessors.containsKey(node))
					{
						Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = ancestorsSuccessors.get(node);
						partialLists.second().add(successor);
						ancestorsSuccessors.replace(node, partialLists);
					}
					else
					{
						SortedSet<Integer> partialSuccessorList = new TreeSet<>();
						SortedSet<Integer> partialAncestorList = new TreeSet<>();
						partialSuccessorList.add(successor);
						Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = new Pair<>(partialAncestorList, partialSuccessorList);
						ancestorsSuccessors.put(node, partialLists);
					}
					if(ancestorsSuccessors.containsKey(successor))
					{
						Pair<SortedSet<Integer>, SortedSet<Integer>> partialLists = ancestorsSuccessors.get(successor);
						partialLists.first().add(node);
						ancestorsSuccessors.replace(successor, partialLists);
					}
					else
					{
						SortedSet<Integer> partialSuccessorList = new TreeSet<>();
						SortedSet<Integer> partialAncestorList = new TreeSet<>();
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
			for(HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> node : entries)
			{
				writer.println(node.getKey().toString() + " " + node.getValue().first().toString() + " " + node.getValue().second().toString());
			}
			writer.close();

			return ancestorsSuccessors;
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE, "Error creating ancestor list", ex);
			return null;
		}
	}

	/**
	 * Encode the list of ancestors and the list of successors of each node in a file called textfile_ancestor_successor_compressed.txt
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public  void encodeAncestorsSuccessors(HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> ancestorSuccessorList)
	{
		Set<HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>>> entries = ancestorSuccessorList.entrySet();
		// compress each node
		for (HashMap.Entry<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> nodeToCompress : entries)
		{
			encodeAncestorsSuccessors(nodeToCompress);
			//System.out.println("Node " + nodeToCompress.getKey() + " encoded.");
		}
	}

	public  boolean putAnnotationEntry(String key_s, String value_s)
	{
		try
		{
			byte[] value = value_s.getBytes("UTF-8");
			return putAnnotationEntry(key_s, value);
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			return false;
		}
	}

	public boolean putAnnotationEntry(String key_s, byte[] value)
	{
		try
		{
			// Create DatabaseEntry for the key
			DatabaseEntry key;
			key = new DatabaseEntry(key_s.getBytes("UTF-8"));
			// Create the DatabaseEntry for the data.
			DatabaseEntry data = new DatabaseEntry(value);
			//vertexBinding.objectToEntry(incomingVertex, data);
			annotationsDatabase.put(null, key, data);

			return true;
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			return false;
		}

	}

	@Override
	public byte[] getAnnotationEntry(String id)
	{
		try
		{
			// Create DatabaseEntry for the key
			DatabaseEntry key = new DatabaseEntry(id.getBytes("UTF-8"));
			// Create the DatabaseEntry for the data.
			DatabaseEntry data = new DatabaseEntry();
			annotationsDatabase.get(null, key, data, LockMode.DEFAULT);
			return data.getData();
		}
		catch (UnsupportedEncodingException ex)
		{
			Logger.getLogger(BerkeleyDB.class.getName()).log(Level.WARNING, null, ex);
		}

		return null;

	}

	public boolean putScaffoldEntry(String key_s, byte[] value)
	{
		try
		{
			if(value != null)
			{
				if(useAdjacencyListCache)
				{
					adjacencyListCache.put(key_s, new String(value));
				}
				// Create DatabaseEntry for the key
				DatabaseEntry key;
				key = new DatabaseEntry(key_s.getBytes("UTF-8"));
				// Create the DatabaseEntry for the data.
				DatabaseEntry data = new DatabaseEntry(value);
				//vertexBinding.objectToEntry(incomingVertex, data);
				scaffoldDatabase.put(null, key, data);
			}
			return true;
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			return false;
		}

	}

	@Override
	public  boolean putScaffoldEntry(String key_s, String value_s)
	{
		try
		{
			byte[] value = value_s.getBytes("UTF-8");
			return putScaffoldEntry(key_s, value);
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			return false;
		}
	}

	public String getScaffoldEntry(String key_s)
	{
		getScaffolds++;
		if(useAdjacencyListCache)
		{
			String value = adjacencyListCache.get(key_s);
			if(value != null)
			{
				adjacencyListCacheHits++;
				return value;
			}
		}
		byte[] aux;
		try
		{
			DatabaseEntry key;
			key = new DatabaseEntry(key_s.getBytes("UTF-8"));
			// Create the DatabaseEntry for the data.
			DatabaseEntry data = new DatabaseEntry();
			scaffoldDatabase.get(null, key, data, LockMode.DEFAULT);
			if(data.getSize() == 0)
				aux = new byte[0];
			else
				aux = data.getData();
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			aux = new byte[0];
		}

		if(aux.length == 0)
			return "";
		else
			return new String(aux);
	}

	public boolean putNext(Database db, String key_s, byte[] newValue, String sep)
	{
		byte[] separatorInput;
		try
		{
			separatorInput = sep.getBytes("UTF-8");
			byte [] separator = new byte[separatorInput.length];
			compressor.setInput(separatorInput);
			compressor.finish();
			compressor.deflate(separator);
			DatabaseEntry key = new DatabaseEntry(key_s.getBytes("UTF-8"));
			DatabaseEntry data = new DatabaseEntry();
			db.get(null, key, data, LockMode.DEFAULT);
			DatabaseEntry data2;
			if (data.getSize() > 0)
			{
				byte[] previousValue = data.getData();
				byte[] value = new byte[previousValue.length + separator.length + newValue.length];
				System.arraycopy(previousValue, 0, value, 0, previousValue.length);
				System.arraycopy(separator, 0, value, previousValue.length, separator.length);
				System.arraycopy(newValue, 0, value, previousValue.length + separator.length, newValue.length);
				data2 = new DatabaseEntry(value);
			}
			else
			{
				data2 = new DatabaseEntry(newValue);
			}
			db.put(null, key, data2);
			return true;
		}
		catch (UnsupportedEncodingException ex)
		{
			logger.log(Level.SEVERE, null, ex);
			return false;
		}

	}


	/*public static Pair<SortedSet<Integer>, SortedSet<Integer>> uncompressAncestorsSuccessors (Integer id, boolean uncompressAncestors, boolean uncompressSuccessors) throws FileNotFoundException, UnsupportedEncodingException {
	SortedSet<Integer> ancestors = new TreeSet<Integer>();
	SortedSet<Integer> successors = new TreeSet<Integer>();
	//LimitedQueue<String> previousLines = new LimitedQueue<String>(W*L); 
	//File file = new File(textfile + "_ancestor_successor_compressed.txt");
	//Scanner sc = new Scanner(file);
	//while (sc.hasNextLine()) {
	//String aux = sc.nextLine();
	//Integer lineID = Integer.parseInt(aux.substring(0, aux.indexOf(" ")));
	//if (lineID.equals(id)) { 
	String aux = getAnnotation(scaffoldDatabase, id);
	if(aux != null) {
		System.out.println("ca marche");
		// split the line in two parts : ancestor list and successor list.
		String ancestorList = aux.substring(0, aux.indexOf('/'));
		String successorList = aux.substring(aux.indexOf('/')+2);
		if (uncompressAncestors) { //uncompressAncestors
			if(ancestorList.contains(" _ ")) { // means there is no reference
				String ancestorList2 = ancestorList.substring(ancestorList.indexOf("_") + 1);
				ancestors.addAll(uncompressRemainingNodes(id, ancestorList2));
			} else { // there is a reference that we have to uncompress
				// uncompress the remaining Nodes
				String remaining = ancestorList.substring(ancestorList.indexOf(" ")+1);
				remaining = remaining.substring(remaining.indexOf(" ")+1);
				ancestors.addAll(uncompressRemainingNodes(id, remaining));
				//uncompress the reference and its reference after that
				ancestors.addAll(uncompressReference(id, ancestorList, true));
			}
		}
		if (uncompressSuccessors) { // uncompressSuccessors
			if(successorList.contains(" _ ")) { // means there is no reference
				String successorList2 = successorList.substring(successorList.indexOf("_")+ 1);
				successors.addAll(uncompressRemainingNodes(id, successorList2));
			} else { // there is a reference that we have to uncompress
				// uncompress the remaining Nodes
				String remaining = successorList.substring(successorList.indexOf(" ")+1);
				remaining = remaining.substring(remaining.indexOf(" ")+1);
				successors.addAll(uncompressRemainingNodes(id, remaining));
				//uncompress the reference and its reference after that
				//System.out.println(successorList);
				successors.addAll(uncompressReference(id, successorList, false));
			}
		}
	}
	//sc.close();
	Pair<SortedSet<Integer>, SortedSet<Integer>> ancestorsAndSuccessors = new Pair<SortedSet<Integer>, SortedSet<Integer>>(ancestors, successors);
	return ancestorsAndSuccessors;
}*/

	/**
	 * Encodes the annotation on edges and vertexes issued by SPADE using dictionary encoding and store it in hashmaps
	 * @param textfile The data issued by SPADE
	 * @return A pair of hashmaps.The first one contains the vertexes, the key is the id of the vertex and the value a byte array containing the encoded annotations.
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public void dictionaryEncoding(String textfile) throws FileNotFoundException, UnsupportedEncodingException {
		//Deflater compressor = new Deflater(Deflater.BEST_COMPRESSION);
		File file = new File(textfile + ".txt");
		Scanner sc = new Scanner(file);
		String sep = "}{";
		byte[] separatorInput = sep.getBytes("UTF-8");
		byte [] separator = new byte[separatorInput.length];
		compressor.setInput(separatorInput);
		compressor.finish();
		compressor.deflate(separator);
		while (sc.hasNextLine()) {
			String toCompress = sc.nextLine();
			if (toCompress.substring(0, 4).equals("EDGE")) {
				String infoToCompress = toCompress.substring(toCompress.indexOf('{') + 1, toCompress.indexOf('}'));
				byte[] input = infoToCompress.getBytes("UTF-8");
				byte [] output = new byte[input.length + 100];
				compressor.setInput(input);
				compressor.finish();
				int compressedDataLength = compressor.deflate(output);
				//System.out.println(compressedDataLength);
				/*	DatabaseEntry key = new DatabaseEntry(toCompress.substring(toCompress.indexOf('(')+1, toCompress.indexOf(')')).getBytes("UTF-8"));
			DatabaseEntry data = new DatabaseEntry();
			annotationsDatabase.getAnnotation(null, key, data, LockMode.DEFAULT);
			DatabaseEntry data2;
			if (data.getSize()>0){
				byte[] previousValue = data.getData();
				byte[] value = new byte[previousValue.length + separator.length + output.length];
				System.arraycopy(previousValue, 0, value, 0, previousValue.length);
				System.arraycopy(separator, 0, value, previousValue.length, separator.length);
				System.arraycopy(output, 0, value, previousValue.length + separator.length, output.length);
				data2 = new DatabaseEntry(value);
			} else {
				data2 = new DatabaseEntry(output);
			}
			annotationsDatabase.put(null, key, data2);*/
				putNext(annotationsDatabase, toCompress.substring(toCompress.indexOf('(')+1, toCompress.indexOf(')')), output, "}{" );
				//	} else {
				//edges.put(key, output);
				//}
				compressor.reset();
			}
			if (toCompress.substring(0, 4).equals("VERT")) {
				String infoToCompress = toCompress.substring(toCompress.indexOf('{') + 1, toCompress.indexOf('}'));
				byte [] input = infoToCompress.getBytes("UTF-8");
				byte [] output = new byte[input.length + 100];
				compressor.setInput(input);
				compressor.finish();
				int compressedDataLength = compressor.deflate(output);
				Integer node = Integer.parseInt(toCompress.substring(toCompress.indexOf('(')+1, toCompress.indexOf(")")));
				/*	DatabaseEntry key = new DatabaseEntry(node.toString().getBytes("UTF-8"));
			DatabaseEntry value = new DatabaseEntry(output);
			annotationsDatabase.put(null, key, value);*/
				putAnnotationEntry(node.toString(), output);
				//System.out.println(compressedDataLength);
				compressor.reset();
			}
		}
		//compressor.end();
		sc.close();
		//Pair<HashMap< Integer, byte[]>, HashMap< String, byte[]>> maps = new Pair<HashMap< Integer, byte[]>, HashMap< String, byte[]>>(vertexes, edges);
		//return maps;
	}


	/**
	 * get the set of annotations of a vertex
	 * @param toDecode node ID
	 * @return the set of annotations as a String
	 * @throws DataFormatException
	 * @throws UnsupportedEncodingException
	 */
	public String decodingVertex(Integer toDecode) throws DataFormatException, UnsupportedEncodingException {
		Inflater decompresser = new Inflater();
		/*DatabaseEntry key = new DatabaseEntry(toDecode.toString().getBytes("UTF-8"));
	DatabaseEntry data = new DatabaseEntry();
	annotationsDatabase.getAnnotation(null, key, data, LockMode.DEFAULT);*/

		//byte[] input = encoded.first().getAnnotation(toDecode);
		byte[] input = getAnnotationEntry(toDecode.toString());
		String outputString;
		//if (data.getSize() == 0) {
		if (input.length == 0) {
			outputString = "Vertex " + toDecode + " does not exist.";
		} else {
			decompresser.setInput(input);
			byte[] result = new byte[1000];
			int resultLength = decompresser.inflate(result);
			decompresser.end();

			// Decode the bytes into a String
			outputString = new String(result, 0, resultLength, "UTF-8");
		}
		//System.out.println(outputString);
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
	public String decodingEdge(Integer node1, Integer node2) throws DataFormatException, UnsupportedEncodingException {
		Inflater decompresser = new Inflater();
		String key_s = node1.toString() + "->" + node2.toString();
		/*DatabaseEntry key = new DatabaseEntry(key_s.getBytes("UTF-8"));
	DatabaseEntry data = new DatabaseEntry();
	annotationsDatabase.getAnnotation(null, key, data, LockMode.DEFAULT);*/
		byte[] input = getAnnotationEntry(key_s);
		String outputString;
		//if (data.getSize() == 0) {
		if (input.length == 0) {
			outputString = "Edge " + node1 + "->" + node2 + " does not exist.";
		} else {
			//byte[]input = data.getData();
			decompresser.setInput(input);
			byte[] result = new byte[1000];
			int resultLength = 
					decompresser.inflate(result);
			decompresser.end();

			// Decode the bytes into a String
			outputString = new String(result, 0, resultLength, "UTF-8");
		}
		//System.out.println(outputString);			
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
	public void renameNodes(String textfile) throws FileNotFoundException, UnsupportedEncodingException{
		String output = textfile + "_preproc.txt";
		File file = new File(textfile + ".txt");
		Scanner sc = new Scanner(file);
		Integer nextID = 0;
		//Vector<String> alreadyRenamed = new Vector<String>();
		PrintWriter writer = new PrintWriter(output, "UTF-8");

		while(sc.hasNextLine()){
			String aux = sc.nextLine();
			if(aux.substring(0,6).equals("VERTEX")) {
				// set new id of the node aka. nextID and put its hashcode in the vector then write the line in file.
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
	public boolean update(String textfile)
	{
		try {
			//renameNodes(textfile);
			HashMap<Integer, Pair<SortedSet<Integer>, SortedSet<Integer>>> asl = createAncestorSuccessorList(textfile);
			updateAncestorsSuccessors(asl);
			dictionaryEncoding(textfile);
			return true;
		} catch (FileNotFoundException | UnsupportedEncodingException e)
		{
			return false;
		}
	}

	@Override
	public Object executeQuery(String query)
	{
		return null;
	}
}
