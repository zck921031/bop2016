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
	String constraint;
	MTGraph(Iterator<Pair<Long,String>> Queries, String Constraint, AtomicLong Workers, 
			ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> Answers, Object B){
		queries = Queries;
		constraint = Constraint;
		workers = Workers;
		answers = Answers;
		b = B;
	}

	public void run(){
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
			//System.out.println(constraint);
			ConcurrentHashMap<Long, String> res = g.getNextNodeByEqual(key, value, constraint);
			if( res.size()>0 ){
				if( !answers.containsKey(key) || value.equals("AA.AuId") ){
					answers.put(key, res);
				}
			}
			System.out.println( query );
		}
	}
	
	public static ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> 
	getNextNodesByEqual(ArrayList<Pair<Long,String>> queries, String constraint){
		long threads = Math.min(16, queries.size());	// 线程数
		Iterator<Pair<Long,String>> itor = queries.iterator();
		AtomicLong workers = new AtomicLong(threads);
		ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> answers =
				new ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>>();
		Object b = new Object();
		MTGraph test = new MTGraph(itor, constraint, workers, answers, b);
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
		Graph g = new Graph();		
		// 正向按照题意走。
		ConcurrentMap<Long, String> hop1 = null;
		ConcurrentMap<Long, String> hop1_AuId = g.getNextNodeByEqual(X, "AA.AuId", "");
		if( hop1_AuId.size()>0 ){
			hop1 = hop1_AuId;
		}else{
			ConcurrentMap<Long, String> hop1_Id = g.getNextNodeByEqual(X, "Id", "");
			hop1 = hop1_Id;
		}
		
		// 反向走，参考文献的边反向，不需要向参考文献连边，而需要查询哪些文章引用了它。
		String constraint;
		ConcurrentMap<Long, String> rhop1 = null;
		ConcurrentMap<Long, String> rhop1_AuId = g.getNextNodeByEqual(Y, "AA.AuId", "");
		if( rhop1_AuId.size()>0 ){
			constraint = Long.toString(Y) + "=AA.AuId";
			rhop1 = rhop1_AuId;
		}else{
			constraint = Long.toString(Y) + "=Id";
			ConcurrentMap<Long, String> rhop1_id = g.getNextNodeByEqual(Y, "Id", "");
			ConcurrentMap<Long, String> rhop1_Rid = g.getNextNodeByEqual(Y, "RId", "");
			rhop1 = rhop1_Rid;
			for( Long key : rhop1_id.keySet() ){
				String value = rhop1_id.get(key);
				if( value.equals("RId") ){
					System.out.println("Reverse solution ignores " + value + "="+key);
					continue;
				}
				rhop1.put(key, value);
			}
		}
		
		// 起点第二跳多线程请求。
		ArrayList<Pair<Long,String>> queries = new ArrayList<Pair<Long,String>>();		
		//ConcurrentHashMap<Long, ConcurrentHashMap<Long,String>> res;
		for(Long key : hop1.keySet() ){
			String value = hop1.get(key);
			value = value.equals("RId")?"Id":value;
			queries.add( new Pair<Long,String>(key,value) );			
		}
		ConcurrentMap<Long, ConcurrentHashMap<Long,String>> hop2 = null;
		hop2 = getNextNodesByEqual(queries, constraint);
		//////////////////		
		// Find answer
		ArrayList<String> json = new ArrayList<String>();
		if ( hop1.containsKey(Y) ){
			json.add( "["+X + "," + Y +"]");
			//System.out.println("Find 1-hop answer: "+ X + " -> " + Y);
		}
		for(Long key1 : hop1.keySet() ){
			//String value1 = hop1.get(key1);
			if ( rhop1.containsKey(key1) ){
				//if( X!=key1 && key1!=Y ){
					json.add( "["+X + "," + key1 +","+ Y +"]");
					//System.out.println("Find 2-hop answer: "+ X + " -> " + key1 +" -> "+ Y);
				//}
			}
			if ( !hop2.containsKey(key1) ) continue;
			for(Long key2 : hop2.get(key1).keySet() ){
				if( rhop1.containsKey(key2) ){
					//if( X!=key1 && X!=key2 && key1!=key2 && key1!=Y && key2!=Y ){
					//if( X!=key1 && X!=key2 && key1!=key2 ){
						json.add( "["+X + "," + key1 +","+ +key2+","+Y +"]");
						//System.out.println("Find 3-hop answer: "+ X + " -> " + key1 + " -> " + key2 + " -> "+ Y);
					//}
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
		//System.out.println( solve(2140251882L, 2145115012L) );
		Long ss = System.currentTimeMillis();
		//System.out.println( solve(2251253715L, 2180737804L) );
		System.out.println( solve(2147152072L, 189831743L) );
		System.out.println( System.currentTimeMillis()-ss );
	}

}
