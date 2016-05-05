package develop;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import javafx.util.Pair;

public class MTGraph implements Runnable{
	AtomicLong workers;
	Iterator<Pair<Long,String>> queries;
	ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> answers;
	Object b;
	MTGraph(Iterator<Pair<Long,String>> Queries, AtomicLong Workers, 
			ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> Answers, Object B){
		queries = Queries;
		workers = Workers;
		answers = Answers;
		b = B;
	}

	public void run(){
//		try {
//			Thread.sleep( (long)(1000*Math.random()) );
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		Graph g = new Graph();
		while ( true ){
			Pair<Long,String> query;
			synchronized (queries){
				if ( queries.hasNext() ){
					query = queries.next();
				}else{
					Long cnt = workers.addAndGet(-1);
					//System.out.println( cnt );
					if ( cnt<=0 ){
						synchronized(b){
							b.notify();
						}
					}
					return;
				}
			}
			Long key = query.getKey();
			String value = query.getValue();
			ConcurrentHashMap<Long, String> res = g.getNextNodeByEqual(key, value);
			if( res.size()>0 ){
				if( !answers.containsKey(key) || value.equals("AA.AuId") ){
					answers.put(key, res);
				}
			}
			System.out.println( query );
		}
	}
	
	public static ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> 
	getNextNodesByEqual(ArrayList<Pair<Long,String>> queries){
		long threads = 80;	// 线程数
		Iterator<Pair<Long,String>> itor = queries.iterator();
		AtomicLong workers = new AtomicLong(threads);
		ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> answers =
				new ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>>();
		Object b = new Object();		
		MTGraph test = new MTGraph(itor, workers, answers, b);
		for (int i=0; i<threads; i++){
			new Thread(test).start();
		}
		synchronized (b){
			try {
				b.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return answers;
	}
	
	public static String solve(long X, long Y){
		ArrayList<Pair<Long,String>> queries;
		ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> res;
		queries = new ArrayList<Pair<Long,String>>();
		queries.add(new Pair<Long,String>(X, "AA.AuId" ));
		queries.add(new Pair<Long,String>(Y, "AA.AuId" ));
		queries.add(new Pair<Long,String>(X, "Id" ));
		queries.add(new Pair<Long,String>(Y, "Id" ));
		res = getNextNodesByEqual(queries);
		ConcurrentMap<Long, String> hop1 = null;
		ConcurrentMap<Long, String> rhop1 = null;
		hop1 = res.get(X);
		rhop1 = res.get(Y);
		
		queries = new ArrayList<Pair<Long,String>>();
		for(Long key : hop1.keySet() ){
			String value = hop1.get(key);
			value = value.equals("RId")?"Id":value;
			queries.add( new Pair<Long,String>(key,value) );			
		}
		ConcurrentMap<Long, ConcurrentHashMap<Long,String>>  hop2;
		hop2 = getNextNodesByEqual(queries);
		
		// Find answer
		ArrayList<String> json = new ArrayList<String>();
		if ( hop1.containsKey(Y) ){
			json.add( "["+X + "," + Y +"]");
			//System.out.println("Find 1-hop answer: "+ X + " -> " + Y);
		}
		for(Long key1 : hop1.keySet() ){
			//String value1 = hop1.get(key1);
			if ( rhop1.containsKey(key1) ){
				if( X!=key1 && key1!=Y ){
					json.add( "["+X + "," + key1 +","+ Y +"]");
					//System.out.println("Find 2-hop answer: "+ X + " -> " + key1 +" -> "+ Y);
				}
			}
			if ( !hop2.containsKey(key1) ) continue;
			for(Long key2 : hop2.get(key1).keySet() ){
				if( rhop1.containsKey(key2) ){
					if( X!=key1 && X!=key2 && key1!=key2 && key1!=Y && key2!=Y ){
						json.add( "["+X + "," + key1 +","+ +key2+","+Y +"]");
						//System.out.println("Find 3-hop answer: "+ X + " -> " + key1 + " -> " + key2 + " -> "+ Y);
					}
				}
			}
		}		
		System.out.println( json.toString() );
		return json.toString();
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		for(int i=0; i<40; i++){
//			queries.add(new Pair(10000000000L+i, "Id" ));
//			queries.add(new Pair(10000000000L-i, "AA.AuId" ));
//		}
		System.out.println( solve(2140251882L, 2145115012L) );
	}

}
