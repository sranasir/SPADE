import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Graph;
import spade.storage.BerkeleyDB;
import spade.storage.CompressedBerkeleyDB;
import spade.storage.CompressedSQL;
import spade.storage.CompressedStorage;
import spade.storage.PostgreSQL;
import spade.storage.TextFile;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Benchmarks
{

	public static void main(String[] args)
    {
		String fileName = "benchmarks/postmark.dot";
        int query_sample_size = 1000;
        String storageName = "PostgreSQL"; // PostgreSQL or BerkeleyDB
        boolean benchmarkQuery = false; // run query benchmark or not
        CompressedStorage storage;

        if(storageName.equalsIgnoreCase("PostgreSQL"))
        {
            storage = new CompressedSQL();
            System.out.println("Working with PostgreSQL.");
        }
        else
        {
            storage = new CompressedBerkeleyDB();
            System.out.println("Working with BerkeleyDB.");
        }
        storage.initialize("benchmarks");
        System.out.println("Importing graph from file: " + fileName);
        Graph graph = Graph.importGraph(fileName);

        Set<String> hashes = new HashSet<>(query_sample_size + 1, 1);
        int count = 0;
        long insertionTime = System.currentTimeMillis();
        assert graph != null;
        for (AbstractVertex v : graph.vertexSet())
        {
            storage.putVertex(v);
            if(count < query_sample_size)
                hashes.add(v.bigHashCode());
            count++;
        }
        CompressedStorage.filePrinter.println("Time to put " + storage.vertexCount() + " vertices in the annotations Database: " +
                1.*(System.currentTimeMillis() - insertionTime)/1e3 + " seconds");
        System.out.println("Time to put " + storage.vertexCount() + " vertices in the annotations Database: " +
                1.*(System.currentTimeMillis() - insertionTime)/1e3 + " seconds");

        count = 0;
        insertionTime = System.currentTimeMillis();
        for (AbstractEdge e : graph.edgeSet())
        {
            storage.putEdge(e);
            count++;
        }

		CompressedStorage.filePrinter.println("Time to put " + storage.edgeCount() + " edges in the annotations Database: " +
                1.*(System.currentTimeMillis() - insertionTime)/1e3 + " seconds");
		System.out.println("Time to put " + storage.edgeCount() + " edges in the annotations Database: " +
                1.*(System.currentTimeMillis() - insertionTime)/1e3 + " seconds");

        if(benchmarkQuery)
            benchmarkLineageQuery(query_sample_size, hashes, storage);

//		System.out.println("adjacencyListCache.size: " + storage.adjacencyListCache.size());
//		System.out.println("getScaffolds: " + storage.getScaffolds);
//		System.out.println("adjacencyListCacheHits: " + storage.adjacencyListCacheHits);
//		System.out.println("Hit Ratio: " + 1.*storage.adjacencyListCacheHits / storage.getScaffolds);
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(12, true, true).second().second().toString());
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(44139, true, true).second().second().toString());
        storage.shutdown();
	}

	private static void benchmarkLineageQuery(int query_sample_size, Set<String> hashes, CompressedStorage storage)
    {
        try
        {
            PrintWriter query = new PrintWriter("benchmarks/query_time.txt");
            List<Double> queryTimes = new ArrayList<>(query_sample_size);
            Graph result = new Graph();
            long total_query_time = 0;
            String direction = "desc";
            int maxDepth = 5;
            for(String hash: hashes)
            {
                long query_clock = System.nanoTime();
                Map<String, Set<String>> lineageMap = storage.getLineageMap(hash, direction, maxDepth);
                result = storage.constructGraphFromLineageMap(lineageMap, direction);
                long query_time = System.nanoTime() - query_clock;
                total_query_time += query_time;
                queryTimes.add(1.*query_time/1e9);
                String result_stats = "Graph stats: vertices=" + result.vertexSet().size() + ", edges=" + result.edgeSet().size();
//                System.out.println(result_stats);
//                query.println(result_stats);
                query.println(result);
            }
            System.out.println("Average query time: " + 1.*(total_query_time/query_sample_size)/1e9);
            query.println("Average query time: " + 1.*(total_query_time/query_sample_size)/1e9);
            System.out.println("Total query time: " + total_query_time);
            query.println("Total query time: " + total_query_time);
            Collections.sort(queryTimes);
            System.out.println(queryTimes.toString());
            query.println(queryTimes.toString());
        }
        catch (FileNotFoundException e1)
        {
            e1.printStackTrace();
        }

    }


    public static void benchmarksBDBStorage()
    {
        AbstractStorage storage = new BerkeleyDB();
        String arguments = "bdb";
        storage.initialize(arguments);

        String workload_file = "benchmarks/audit.log4m.dot";
        Graph graph = Graph.importGraph(workload_file);
        Set<String> hashes = new HashSet<>(1000+1, 1);
        int count = 0;
        long aux = System.currentTimeMillis();
        for (AbstractVertex v : graph.vertexSet())
        {
            storage.putVertex(v);
            if(count < 1000)
                hashes.add(v.bigHashCode());
            count++;
        }
        System.out.println("Time to put all vertices in the annotations Database: " + 1.*(System.currentTimeMillis() - aux)/1e3);

        aux = System.currentTimeMillis();
        for (AbstractEdge e : graph.edgeSet())
        {
            storage.putEdge(e);
        }
        storage.scaffold.globalTxCheckin(true);
        System.out.println("Time to put all edges in the annotations Database: " + 1.*(System.currentTimeMillis() - aux)/1e3);

        try
        {
            PrintWriter query = new PrintWriter("benchmarks/query_time.txt");
            long query_time = 0;
            int countLines = 0;
            for(String hash: hashes)
            {
                countLines++;
                long aux_query1 = System.nanoTime();
                storage.scaffold.getLineage(hash, "desc", 5);
                long aux_query2 = System.nanoTime();
                query.println(aux_query2 - aux_query1);
                query_time += aux_query2 - aux_query1;
            }
            System.out.println("Average query time: " + 1.*(query_time/countLines)/1e9);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public static void benchmarksSQLStorage()
    {
        AbstractStorage storage = new PostgreSQL();
        String arguments = "annotations raza 12345";
        storage.initialize(arguments);

        String workload_file = "benchmarks/audit.log4m.dot";
        Graph graph = Graph.importGraph(workload_file);
        Set<String> hashes = new HashSet<>(1000+1, 1);
        int count = 0;
        long aux = System.currentTimeMillis();
        for (AbstractVertex v : graph.vertexSet())
        {
            storage.putVertex(v);
            if(count < 1000)
                hashes.add(v.bigHashCode());
            count++;
        }
        System.out.println("Time to put all vertices in the annotations Database: " + 1.*(System.currentTimeMillis() - aux)/1e3);

        aux = System.currentTimeMillis();
        for (AbstractEdge e : graph.edgeSet())
        {
            storage.putEdge(e);
        }
        storage.scaffold.globalTxCheckin(true);
        System.out.println("Time to put all edges in the annotations Database: " + 1.*(System.currentTimeMillis() - aux)/1e3);

        try
        {
            PrintWriter query = new PrintWriter("benchmarks/query_time.txt");
            long query_time = 0;
            int countLines = 0;
            for(String hash: hashes)
            {
                countLines++;
                long aux_query1 = System.nanoTime();
                storage.scaffold.getLineage(hash, "desc", 5);
                long aux_query2 = System.nanoTime();
                query.println(aux_query2 - aux_query1);
                query_time += aux_query2 - aux_query1;
            }
            System.out.println("Average query time (ns) : " + (query_time/countLines));
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
	
	public static void benchmarksTextfile() {
		TextFile storage = new TextFile();
		storage.initialize("benchmark.txt");
        Graph graph = Graph.importGraph("benchmarks/audit.log4m.dot");
        System.out.println("Total Vertices: " + graph.vertexSet().size());
        System.out.println("Total Edges: " + graph.edgeSet().size());
        System.out.println("starting putVertex");
        //int count = 0;
        long aux = System.currentTimeMillis();
        for (AbstractVertex v : graph.vertexSet()) {
            storage.putVertex(v);
            //System.out.print(count + " ");
            //count ++;
        }
        System.out.println("Time to put all vertices in the database (ms): " + (System.currentTimeMillis() - aux));
        //count = 0;
        aux = System.currentTimeMillis();
        
        for (AbstractEdge e : graph.edgeSet()) {
            storage.putEdge(e);
            //System.out.print(count + " ");
            //count ++;
        }
        System.out.println("Time to put all edges in the database (ms): " + (System.currentTimeMillis() - aux));
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(12, true, true).second().second().toString());
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(44139, true, true).second().second().toString());
	}
}
