import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Graph;
import spade.storage.BerkeleyDB;
import spade.storage.CompressedStorage;
import spade.storage.CompressedStorageSQL;
import spade.storage.PostgreSQL;
import spade.storage.TextFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Benchmarks {

	public static void main(String[] args) {
		
		benchmarksCompressedStorage();
//		benchmarksTextfile();
//      benchmarksSQLStorage();
//        benchmarksBDBStorage();



        
       /*TextFileRenamed storage = new TextFileRenamed();
		storage.initialize("/Users/melanie/Documents/benchmarks2/textfile.txt");
        Graph graph = Graph.importGraph("/Users/melanie/Documents/benchmarks/workload.dot");
        System.out.println("starting putVertex");
        //int count = 0;
        for (AbstractVertex v : graph.vertexSet()) {
            storage.putVertex(v);
            //System.out.print(count + " ");
            //count ++;
        }
        //count = 0;
        System.out.println("starting putEdge");
        for (AbstractEdge e : graph.edgeSet()) {
            storage.putEdge(e);
            //System.out.print(count + " ");
            //count ++;
        }
        System.out.println("shutdown");
        storage.shutdown(); */
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
        System.out.println("Time to put all vertices in the annotations Database (ms): " + (System.currentTimeMillis() - aux));

        aux = System.currentTimeMillis();
        for (AbstractEdge e : graph.edgeSet())
        {
            storage.putEdge(e);
        }
        storage.scaffold.globalTxCheckin(true);
        System.out.println("Time to put all edges in the annotations Database (ms): " + (System.currentTimeMillis() - aux));

        try {
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
        catch (Exception ex) {
            // TODO Auto-generated catch block
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
        System.out.println("Time to put all vertices in the annotations Database (ms): " + (System.currentTimeMillis() - aux));

        aux = System.currentTimeMillis();
        for (AbstractEdge e : graph.edgeSet())
        {
            storage.putEdge(e);
        }
        storage.scaffold.globalTxCheckin(true);
        System.out.println("Time to put all edges in the annotations Database (ms): " + (System.currentTimeMillis() - aux));

        try {
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
        catch (Exception ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }
    }
	
	public static void benchmarksCompressedStorage() {
		CompressedStorage storage = new CompressedStorage();
//		CompressedStorageSQL storage = new CompressedStorageSQL();
		storage.initialize("benchmarks");
		String workload_file = "benchmarks/audit.log4m.dot";
        Graph graph = Graph.importGraph(workload_file);
        Set<String> hashes = new HashSet<>(1000+1, 1);
        int count = 0;
        long aux = System.currentTimeMillis();
        for (AbstractVertex v : graph.vertexSet()) {
            storage.putVertex(v);
            if(count < 1000)
                hashes.add(v.bigHashCode());
            count++;
            //System.out.print(count + " ");
            //count ++;
        }
        storage.benchmarks.println("Time to put all vertices in the annotations Database (ms): " + (System.currentTimeMillis() - aux));
        System.out.println("Time to put all vertices in the annotations Database (ms): " + (System.currentTimeMillis() - aux));
        //count = 0;
        aux = System.currentTimeMillis();
        
        for (AbstractEdge e : graph.edgeSet()) {
            storage.putEdge(e);
            //System.out.print(count + " ");
            //count ++;
        }
//        storage.flushBulkScaffold(true);
		storage.benchmarks.println("Time to put all edges in the annotations Database (ms): " + (System.currentTimeMillis() - aux));
		System.out.println("Time to put all edges in the annotations Database (ms): " + (System.currentTimeMillis() - aux));
//		System.out.println("compressedScaffold.size: " + storage.compressedScaffold.size());
//		System.out.println("getScaffolds: " + storage.getScaffolds);
//		System.out.println("compressedScaffoldHits: " + storage.compressedScaffoldHits);
//		System.out.println("Hit Ratio: " + 1.*storage.compressedScaffoldHits / storage.getScaffolds);

//		System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(12, true, true).second().second().toString());
        /*try {
			System.out.println(storage.getTime(39363, 39011));
		} catch (UnsupportedEncodingException | DataFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
       
//        File file = new File("benchmarks/hashes.scaffold");

        try {
        	PrintWriter query = new PrintWriter("benchmarks/query_time.txt");
        	long query_time = 0;
//			Scanner sc = new Scanner(file);
			int countLines = 0;
			for(String hash: hashes)
            {
				countLines++;
				long aux_query1 = System.nanoTime();
				storage.getLineageMap(hash, "desc", 5);
				long aux_query2 = System.nanoTime();
				query.println(aux_query2 - aux_query1);
				query_time += aux_query2 - aux_query1;
			}
			System.out.println("Average query time (ns) : " + (query_time/countLines));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(12, true, true).second().second().toString());
        //System.out.println(storage.uncompressAncestorsSuccessorsWithLayer(44139, true, true).second().second().toString());
        storage.shutdown();
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
