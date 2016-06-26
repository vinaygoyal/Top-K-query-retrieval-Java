package source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
public class topk {

	public static int K;
	public static int N;
	public static int M;
	public static int blockSize;
	public static BTree[] indices;
	public static String[] header;

	public class Data {
		private int ID;
		private int score;

		public Data(int _ID, int _score) {
			ID = _ID;
			score = _score;
		}
	}

	public class DataComparator implements Comparator<Data> {
		@Override
		public int compare(Data o1, Data o2) {
			if (o1.score > o2.score) {
				return 1;
			}
			return -1;
		}
	}

	private static ArrayList<ArrayList<Integer>> Table = new ArrayList<ArrayList<Integer>>();

	// create indexes for single file
	public static BTree[] init(File fileName) throws NumberFormatException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line = reader.readLine();
		header = line.split(",");
		while ((line = reader.readLine()) != null) {
			String[] vals = line.split(",");
			ArrayList<Integer> tuple = new ArrayList<Integer>();

			// relation has N+1 attributes
			for (int i = 0; i < N + 1; ++i) {
				tuple.add(Integer.parseInt(vals[i]));
			}
			Table.add(tuple);
		}
		reader.close();

		// dense primary index on ID
		BTree[] indexes = new BTree[N + 1]; // size same as that of the score length
		for (int i = 0; i < indexes.length; ++i) {
			indexes[i] = new BTree(blockSize);
		}

		for (int i = 0; i < Table.size(); ++i) {
			ArrayList<Integer> tuple = Table.get(i);
			for (int j = 0; j < tuple.size(); ++j) {
				// primary index
				indexes[0].insert(tuple.get(0), tuple.get(j));

				if (j >= 1) {
					indexes[j].insert(tuple.get(j), tuple.get(0));
				}
			}
		}		
		return indexes;
	}

	// join table
	public static ArrayList<ArrayList<Integer>> joinTable(ArrayList<Map<String, Integer>> tableMap) throws IOException {
		ArrayList<ArrayList<ArrayList<Integer> > >  tables = new ArrayList<ArrayList<ArrayList<Integer> > >(); 
		ArrayList<Integer> columnIDs = new ArrayList<Integer>();
		ArrayList<String> title = new ArrayList<String>();

		if(tableMap.size() < 2) {
			System.out.println("Minimum 2 tables required to perform join.");
			return new ArrayList<ArrayList<Integer>>();
		}
		
		for(int i = 0; i < tableMap.size(); ++i) {
			Map<String, Integer> entry = tableMap.get(i);
			String fileName = entry.keySet().iterator().next();

			// arrayList of tables
			ArrayList<ArrayList<Integer>> tab = new ArrayList<ArrayList<Integer>>();
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine();
						
			String[] temp = line.split(",");
			for(int m = 0; m < N+1; ++m) {
				title.add(temp[m]);
			}
			
			while ((line = reader.readLine()) != null) {
				String[] vals = line.split(",");
				ArrayList<Integer> tuple = new ArrayList<Integer>();

				// relation has N+1 attributes
				for (int j = 0; j < N+1; ++j) {
					tuple.add(Integer.parseInt(vals[j]));
				}
				tab.add(tuple);
			}
			tables.add(tab);
			columnIDs.add(tableMap.get(i).get(fileName));
			reader.close();
		}
				
		// arrayList of map. index matches tables arrayList
		ArrayList<HashMap<Integer, ArrayList<Integer>> > hash = new ArrayList<HashMap<Integer, ArrayList<Integer>> >(); 
		
		for(int i = 0; i < tables.size(); ++i) {
			HashMap<Integer, ArrayList<Integer> > h = new HashMap<Integer, ArrayList<Integer> >();
			hash.add(h);
			// table at index i
			ArrayList<ArrayList<Integer> > table = tables.get(i);
			for(int j = 0; j < table.size(); ++j) {
				ArrayList<Integer> tuple = table.get(j);
				// get hash corresponding to table at i
				HashMap<Integer, ArrayList<Integer> >tableHash = hash.get(i); 
				Integer colID = columnIDs.get(i);
				ArrayList<Integer> values = tableHash.get(tuple.get(colID));
				if(values == null) {
					tableHash.put(tuple.get(colID), new ArrayList<Integer>());
					values = tableHash.get(tuple.get(colID));
				}
				values.add(tuple.get(0)); // pushing oid			
			}			
		}
		
		// we now have hash of size m with mapping of join values with respective OIDs
		
		Set<Integer> filtered = new HashSet<Integer>();
		//intersection is the set of join values in first table.
		Set<Integer> intersection = hash.get(0).keySet();
		
		for(int i = 1; i < hash.size(); ++i) {
			filtered.clear();
			Set<Integer> keys = hash.get(i).keySet();
			Iterator<Integer> it = keys.iterator();
			while(it.hasNext()) {
				Integer k = it.next();
				if(intersection.contains(k) == true) {
					//add to new container
					filtered.add(k);
				}
			}
			intersection = new TreeSet<Integer>(filtered);
		}
		
		// intersection will have keys common in all the tables
		Iterator<Integer> it = intersection.iterator();
		ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();

		while(it.hasNext()) {
			// value is the common attribute
			Integer value = it.next();
			
			ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
			for(int j = 0; j < hash.size(); ++j) { // j is the table index
				HashMap<Integer, ArrayList<Integer> >tableIndex = hash.get(j); // table at index j
				ArrayList<Integer> oids = tableIndex.get(value);
				tuples.add(oids);
			}
			
			// try to find permutations of tuples now.
			ArrayList<Integer> tempResult = new ArrayList<Integer>(tuples.size());
			findPermutations(0, tuples, result, tempResult, tables);
		}
				
		ArrayList<ArrayList<Integer>> joinedTable = new ArrayList<ArrayList<Integer>>();
		for(int i = 0; i < result.size(); ++i) {
			// need to merge all these oids
			ArrayList<Integer> oids = result.get(i);
			
			ArrayList<Integer> joinedTuple = new ArrayList<Integer>();
			joinedTuple.add(i);

			for(int j = 0; j < oids.size(); ++j) {
				Integer OID = oids.get(j); // oid.. query in table at index j
				ArrayList<Integer> tuple = tables.get(j).get(OID);
				joinedTuple.addAll(tuple);
			}
			joinedTable.add(joinedTuple);		
		}	
		header = title.toString().split(",");
		return joinedTable;
	}
	
	//create indexes for multiple files
	public static BTree[] init(ArrayList<Map<String, Integer>> tableMap) throws NumberFormatException, IOException {
		header = new String[tableMap.size()*(N+1)];		
		ArrayList<ArrayList<Integer>> table = joinTable(tableMap);
		Table = table;
		M = tableMap.size();
		N = M*N;
	
		// dense primary index on ID
		BTree[] indexes = new BTree[N + 1]; // size same as that of the score length
		for (int i = 0; i < indexes.length; ++i) {
			indexes[i] = new BTree(blockSize);
		}
				
		for (int i = 0; i < table.size(); ++i) {
			int k = 1;
			ArrayList<Integer> tuple = table.get(i);
			for (int j = 0; j < tuple.size(); ++j) {
				// primary index
				indexes[0].insert(tuple.get(0), tuple.get(j)); // j = 0 means oid in both single and joined

				// don;t create ids for j which forms oids of individual tables
				if (j % (N/M + 1) != 1 && j != 0) {
					indexes[k].insert(tuple.get(j), tuple.get(0));
					k++;
				}
			}
		}
		return indexes;
	}
	
	static void findPermutations(int n, ArrayList<ArrayList<Integer>> tuples, ArrayList<ArrayList<Integer>> result, ArrayList<Integer> tempResult, ArrayList<ArrayList<ArrayList<Integer> > >  tables) {
		if(n == tuples.size()) {
			result.add(new ArrayList<Integer>(tempResult));
			return;
		}
		
		ArrayList<Integer> tuple = tuples.get(n);
		for(int j = 0; j < tuple.size(); ++j) {
			tempResult.add(tuple.get(j));
			findPermutations(n+1, tuples, result, tempResult, tables);
			tempResult.remove(tempResult.size()-1);
		}
	}

	// Fagin's Threshold Algorithm
	public void run1(int[] score, BTree[] indices) {
		topk obj1 = new topk();
		boolean flag = true;
		PriorityQueue<Data> PQueue = new PriorityQueue<Data>(K, new DataComparator());

		int row = 1;
		ArrayList<Integer> idsAdded = new ArrayList<Integer>();
		ArrayList<ArrayList<Integer>> indexID = new ArrayList<ArrayList<Integer>>();
		int[] colSize = new int[indices.length - 1];

		for (int i = 0; i < N+M; i++) {
			System.out.format("%10s", header[i]);
		}
		System.out.format("%10s", "Score");

		System.out.println("");

		while (flag) {

			int[] key = new int[indices.length - 1];
			ArrayList<Integer> idsSeen = new ArrayList<Integer>();
			int thresholdScore = 0;
			ArrayList<Integer> ids = new ArrayList<Integer>();

			for (int i = 1; i < indices.length; i++) {
				BTree index = indices[i];
				if (colSize[i - 1] <= row) {

					BTreeNode rightmostLeaf = index.root.reachRightmostLeaf(); // rightmost node of index

					Entry obj = rightmostLeaf.traverseReverse(row);
					colSize[i - 1] = colSize[i - 1] + obj.getValue().size();
					key[i - 1] = obj.getKey();

					indexID.add(i - 1, obj.getValue()); // ids at the current
														// position
					ids = indexID.get(i - 1); // same ids
				}

				Iterator itr = ids.iterator();
				while (itr.hasNext()) {
					int val = (Integer) itr.next();
					if (!idsSeen.contains(val)) {
						idsSeen.add(val);
					}
				}
			}

			// ***** populate idsSeen for a particular row *****

			// calculate the threshold score of current row
			for (int i = 1; i < indices.length; i++) {
				thresholdScore = thresholdScore + score[i - 1] * key[i - 1];
			}

			Iterator it = idsSeen.iterator();
			while (it.hasNext()) {

				int id = (Integer) it.next();

				int idScore = getScore(score, id); // calculate the score for an
													// index id
				Data data = new Data(id, idScore);

				if (idsAdded.contains(id)) { // check if tuple is already added
												// to priority queue
					continue;
				} else {
					if (PQueue.size() < K && idScore <= thresholdScore) {
						PQueue.add(data);
						idsAdded.add(id);
					} else if (PQueue.size() >= K && idScore > PQueue.peek().score && idScore <= thresholdScore) {
						PQueue.poll();
						PQueue.add(data);
						idsAdded.add(id);
					}

					if (PQueue.size() >= K && idScore >= thresholdScore) {
						flag = false;
						break;
					}
				}
			}
			row++;
		}

		// print the PQueue
		while (PQueue.peek() != null) {
			Data data = PQueue.poll();
			obj1.printTuple(data.ID);
			// System.out.println("");
			System.out.format("%10s", Integer.toString(data.score));
			System.out.println("");
		}
	}

	// Naive Priority Queue Algorithm
	public void run2(int[] score) {

		topk obj1 = new topk();
		PriorityQueue<Data> PQueue = new PriorityQueue<Data>(K, new DataComparator());

		for (int i = 0; i < N+M; i++) {
			System.out.format("%10s", header[i]);
		}
		System.out.format("%10s", "Score");

		System.out.println("");

		for (int i = 0; i < Table.size(); ++i) {
			ArrayList<Integer> row = Table.get(i);

			int rowScore = 0;
			int ID = row.get(0); // OID in single as well as joined table

			int k = 0;
			for (int j = 1; j < row.size(); j++) {
				if(j % (N/M + 1) != 1 || (M==1 && j >=1)) {
					rowScore += row.get(j) * score[k];
					k++;
				}
			}

			// if PQueue is full, compare with head.. remove or ignore current
			// row
			if (PQueue.size() > K - 1) {
				if (rowScore > PQueue.peek().score) {
					PQueue.poll();
				} else {
					// if PQ is full and new score is less than the minimum, we
					// don't need to push it to the queue
					continue;
				}
			}
			PQueue.add(new Data(ID, rowScore));
		}

		// print the PQueue
		while (PQueue.peek() != null) {
			Data data = PQueue.poll();
			
			//ArrayList<Integer> rowPrint = new ArrayList<Integer>(Table.get(data.ID));
			ArrayList<Integer> rowPrint = new ArrayList<Integer>(indices[0].search(data.ID));
			Iterator itr = rowPrint.iterator();
			if(M!=1){
				itr.next();
			}
			while (itr.hasNext()) {
				System.out.format("%10d", itr.next());
			}
			
			System.out.format("%10s", Integer.toString(data.score));
			System.out.println("");
		}
	}

	// Function to calculate the score of a tuple based on primary index id
	public int getScore(int[] score, int id) {

		ArrayList<Integer> rowPrint = new ArrayList<Integer>(indices[0].search(id));
		int rowScore = 0;
		int k = 0;
		for (int j = 1; j < rowPrint.size(); j++) {
			if((j % (N/M + 1) != 1 && M != 1) || (j % (N/M + 1) != 0 && M == 1) ) {   // check if query is normal or join query
				rowScore = rowScore + rowPrint.get(j) * score[k];
				k++;
			}
		}

		return rowScore;

	}

	// Function to print a tuple based on primary index id
	public void printTuple(int index) {
		// Print a row of input csv file
		ArrayList<Integer> rowPrint = new ArrayList<Integer>(indices[0].search(index));
		Iterator itr = rowPrint.iterator();
		if(M!=1){		// check if query is normal or join query
			itr.next();
		}
		while (itr.hasNext()) {
			System.out.format("%10d", itr.next());
		}
	}
	
	public void callInitJoin(String[] input, String[] fileName) throws IOException {
		
		ArrayList<Map<String, Integer>> tableMap = new ArrayList<Map<String, Integer>>();
		HashMap<String, String[]> headers = new HashMap<String, String[]>();
			
				for(int i = 0; i< fileName.length; i++){
					File file = new File(fileName[i]);
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String line = reader.readLine();
					String[] headJoin = line.split(",");

					String fN = fileName[i].substring(fileName[i].indexOf("/") +6, fileName[i].indexOf("."));					
					headers.put(fN, headJoin);		// push filenames and their corresponding headers	
				}
		
		HashMap<String, String> joinInp = new HashMap<String, String>();
		
		for(int i = 3; i < input.length; i++){
			String test = input[i];
			String[] condition = test.split("\\.");
			joinInp.put( condition[0], condition[1]);
			i++;
		}
		
		for(String tab : joinInp.keySet()){
			String col = joinInp.get(tab);			
			String[] head = headers.get(tab);
			int val = Arrays.asList(head).indexOf(col);	 // find index of join condition column 
			Map<String, Integer> t = new HashMap<String, Integer>();
			t.put("src/data/" + tab + ".csv", val);		// push table name and its query column index position
			
			tableMap.add(t);			
		}
		
		indices = init(tableMap);
	}

	public static void main(String[] args) throws NumberFormatException, IOException {

		topk topK = new topk();
		blockSize = 200;

		K = Integer.parseInt(args[0]);
		N = Integer.parseInt(args[1]);
		M = 1;

		indices = new BTree[N + 1];

		Scanner reader = new Scanner(System.in);
		String[] input2 = reader.nextLine().split(" ");

		if (input2[0].equalsIgnoreCase("init")) {
			if(input2[1].contains(",")){		// call init for join query
				String[] filenames = input2[1].split(",");
				topK.callInitJoin(input2, filenames);
				
			}
			else {
				File fileName = new File(input2[1]);	// call init for single table query
				indices = init(fileName);
			}
		}

		reader = new Scanner(System.in);
		String[] input3 = reader.nextLine().split(" ");

		// Calling function run1
		if (input3[0].equals("run1")) {
			int[] scores = new int[input3.length - 1];

			for (int i = 0; i < input3.length - 1; i++) {
				scores[i] = Integer.parseInt(input3[i + 1]);
			}
			topK.run1(scores, indices);
		}

		// Calling function run2
		if (input3[0].equals("run2")) {
			int[] scores = new int[input3.length - 1];

			for (int i = 0; i < input3.length - 1; i++) {
				scores[i] = Integer.parseInt(input3[i + 1]);
			}
			topK.run2(scores);
		}
		reader.close();
	}
}
