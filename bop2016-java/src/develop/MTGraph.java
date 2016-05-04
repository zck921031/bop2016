package develop;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import javafx.util.Pair;

public class MTGraph implements Runnable{
	AtomicLong workers;
	Iterator<Pair<Long,String>> queries;
	Object b;
	MTGraph(Iterator<Pair<Long,String>> Queries, AtomicLong Workers, Object B){
		queries = Queries;
		workers = Workers;
		b = B;
	}
	public void run(){
		try {
			Thread.sleep( (long)(1000*Math.random()) );
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while ( true ){
			Pair<Long,String> query;
			synchronized (queries){
				if ( queries.hasNext() ){
					query = queries.next();
				}else{
					Long cnt = workers.addAndGet(-1);
					System.out.println( cnt );
					if ( cnt<=0 ){
						synchronized(b){
							b.notify();
						}
					}
					return;
				}
			}
			System.out.println( query );
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList queries = new ArrayList<Pair<Long,String>>();
		for(int i=0; i<400; i++){
			queries.add(new Pair(10000000000L+i, "Id" ));
			queries.add(new Pair(10000000000L-i, "AA.AuId" ));
		}
		Iterator itor = queries.iterator();
		long threads = 80;	// 线程数
		AtomicLong workers = new AtomicLong(threads);
		Object b = new Object();
		MTGraph test = new MTGraph(itor, workers, b);
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
	}

}
