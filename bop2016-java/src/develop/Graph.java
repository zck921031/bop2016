package develop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.*;

public class Graph {
	List<NameValuePair> nvps = new ArrayList<NameValuePair>();
	HttpClient httpclient = HttpClients.createDefault();
	HashMap<String, String> queryString=
			new HashMap< String, String >();
	HashMap<String, HashMap<String,String>> attributes = new HashMap<String, HashMap<String,String>>(); 
	Graph(){
	    nvps.add( new BasicNameValuePair("count", "1000000") );
	    nvps.add( new BasicNameValuePair("subscription-key", "f7cc29509a8443c5b3a5e56b0e38b5a6") );
	    queryString.put("Id", "F.FId,J.JId,C.CId,AA.AuId,RId" );
	    queryString.put("F.FId", "Id" );
	    queryString.put("J.JId", "Id" );
	    queryString.put("C.CId", "Id" );
	    queryString.put("AA.AuId", "Id,AA.AuId,AA.AfId" );
	    queryString.put("AA.AfId", "AA.AuId" );
	    queryString.put("RId", "Id" );
	    for( String key : queryString.keySet() ){
	    	HashMap<String,String> hm = new HashMap<String,String>();
	    	for( String value : queryString.get(key).split(",") ){
	    		hm.put(value, "");
	    	}
	    	attributes.put(key, hm);
	    }
	    //System.out.println(queryString);
	}
	
	final static boolean isBasicType(Object value){
		return ( String.class.isInstance(value) ||
				BigInteger.class.isInstance(value) ||
				Long.class.isInstance(value) ||
				Integer.class.isInstance(value) ||
				BigDecimal.class.isInstance(value) ||
				Double.class.isInstance(value) ||
				Boolean.class.isInstance(value) );
	}
	/**
	 * 
	 * @param obj	JSON结构
	 * @param prefix	Key前缀
	 * @param kvs	解析结果 (key,value)
	 */
	static void json_dfs(Object obj, String prefix, ArrayList< Pair<String,String> > kvs){
		if ( obj instanceof JSONObject ){
			//for( String key : ((JSONObject) obj).keySet() ){
			Iterator<String> itor = ((JSONObject) obj).keys();
			while( itor.hasNext() ){
				String key = itor.next();
				Object value = ((JSONObject) obj).get(key);
				String dot = "";
				if ( prefix.length()>0 ) dot=".";
				json_dfs(value, prefix+dot+key, kvs);
			}
		}else if ( obj instanceof JSONArray ){
			for( Object t : ((JSONArray) obj) ){
				json_dfs(t, prefix, kvs);
			}
		}else if (isBasicType(obj)){
			//System.out.println( prefix + "," + obj.toString() );
			kvs.add( new Pair<String,String>(prefix, obj.toString()));
		}else{
			System.err.println("Unkown json structure... Type is " + obj.getClass() );
		}
	}
	
	public ConcurrentHashMap<Long, String> getNextNodeByEqual(Long id, String Name, String constraint){
		ConcurrentHashMap<Long, String> nodes = new ConcurrentHashMap<Long, String>();
		String expr;
		if( Name.equals("Id") || Name.equals("RId")  ){
			expr = Name+"="+ id.toString();
		}else{
			expr = "Composite("+Name+"="+id.toString()+")";
		}
		
		if ( constraint.length()>0 ){
			String[] strs = constraint.split("=");
			String id2 = strs[0];
			String name2 = strs[1];
			if( name2.equals("Id") ){
				if( Name.equals("F.FId") || Name.equals("J.JId") || Name.equals("C.CId") ){
					expr = "AND("+ expr +",RId="+ id2 + ")";
					//System.out.println(expr);
				}
			}
		}
		Long start_time = System.currentTimeMillis();
		try
		{
		    URIBuilder builder = new URIBuilder("http://oxfordhk.azure-api.net/academic/v1.0/evaluate");
			builder.setParameters(nvps);
			builder.setParameter("expr", expr );
			builder.setParameter("attributes", queryString.get(Name) );
			System.out.println( builder.toString() );
		    URI uri = builder.build();
		    HttpGet request = new HttpGet(uri);
		
		    HttpResponse response = httpclient.execute(request);
		    HttpEntity entity = response.getEntity();
		    String json = EntityUtils.toString(entity); 
		    if (entity != null) 
		    {
		        //System.out.println(json);
		        System.out.println("Http Used time is " + (System.currentTimeMillis() - start_time) + " ms" );
		        start_time = System.currentTimeMillis();
		    }
		    ArrayList< Pair<String,String> > kvs = new ArrayList< Pair<String,String> >();
		    json_dfs( new JSONObject(json), "", kvs);
		    System.out.println("Parse Used time is " + (System.currentTimeMillis() - start_time) + " ms" );
		    start_time = System.currentTimeMillis();
		    
		    HashMap<String,?> attr = attributes.get(Name);
		    if( Name.equals("AA.AuId") ){
		    	System.out.println("Special case for "+Name);
		    	boolean ban = false;
		    	for( Pair<String,String> term : kvs ){
			    	String key = term.getKey();
			    	String value = term.getValue();
			    	if( !key.startsWith("entities.") ) continue; 	
		    		key=key.substring(9);
		    		if( !attr.containsKey(key) ) continue;
		    		if( key.equals("AA.AuId") ){
		    			if ( Long.valueOf(value).equals(id) ) {
		    				//System.out.println(key+","+value+" id="+id);
		    				ban = false;
		    			}
		    			else{
		    				//System.out.println(key+","+value);
		    				ban = true;
		    			}
		    			continue;
		    		}else if ( key.equals("Id") ){
		    			ban = false;
		    		}
	    			//System.out.println( key + "," + value + "  ban="+ban);
		    		if ( !ban ){
				    	nodes.put(Long.valueOf(value), key);
				    	//System.out.println( key + "," + value );
		    		}
			    }
		    }else{
			    for( Pair<String,String> term : kvs ){
			    	//System.out.println(term);
			    	String key = term.getKey();
			    	String value = term.getValue();
			    	if( !key.startsWith("entities.") ) continue;		    	
		    		key=key.substring(9);
		    		if( !attr.containsKey(key) ) continue;	    	
			    	nodes.put(Long.valueOf(value), key);
			    	//System.out.println( key + "," + value ); 	
			    }
		    }

//			System.out.println("result size : "+nodes.size());
//			System.out.println(nodes);
//			System.out.println("");
		}
		catch (Exception e)
		{
		    System.err.println(e.getMessage());
		}
		return nodes;
	}
//	public static ConcurrentMap<Long,String> getNextNode(Long k){
//		ConcurrentMap<Long, String> chs = new ConcurrentHashMap<Long, String>();
//		
//		Graph g = new Graph();
//		chs = g.getNextNodeByEqual(k, "Id");
//		//g.getNextNodeForId(k);
//		
//		return chs;
//		
//	}
	public static void main(String[] args) throws FileNotFoundException 
    {
//		System.out.println( new Graph().getNextNodeByEqual(2140251882L, "Id") );
//		System.out.println( new Graph().getNextNodeByEqual(2145115012L, "AA.AuId") );
//		if(1==1) return; 
//		System.setOut(new PrintStream(new File("D:/output-file.txt")));
		//long X = 2140251882L, Y = 2140251882L; 
		//long X = 2140251882L, Y = 2145115012L;
		long X = 2147152072L, Y = 189831743L;
		//long Y = 2140251882L, X = 2145115012L;
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
		
		ConcurrentMap<Long, ConcurrentMap<Long,String> > hop2 =
				new ConcurrentHashMap<Long, ConcurrentMap<Long,String> >();
		for(Long key : hop1.keySet()){
			String value = hop1.get(key);
			value = value.equals("RId")?"Id":value;
			hop2.put(key, g.getNextNodeByEqual(key, value, "") );
		}
		
		// 反向走，参考文献的边反向，不需要向参考文献连边，而需要查询哪些文章引用了它。
		ConcurrentMap<Long, String> rhop1 = null;
		ConcurrentMap<Long, String> rhop1_AuId = g.getNextNodeByEqual(Y, "AA.AuId", "");
		if( rhop1_AuId.size()>0 ){
			rhop1 = rhop1_AuId;
		}else{
			ConcurrentMap<Long, String> rhop1_id = g.getNextNodeByEqual(Y, "Id", "");
			ConcurrentMap<Long, String> rhop1_Rid = g.getNextNodeByEqual(Y, "RId", "");
			System.out.println( rhop1_id.size() );
			System.out.println( rhop1_Rid.size() );
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
		
		if ( hop1.containsKey(Y) ){
			System.out.println("Find 1-hop answer: "+ X + " -> " + Y);
		}
		for(Long key1 : hop1.keySet() ){
			//String value1 = hop1.get(key1);
			if ( rhop1.containsKey(key1) ){
				System.out.println("Find 2-hop answer: "+ X + " -> " + key1 +" -> "+ Y);
			}
			for(Long key2 : hop2.get(key1).keySet() ){
				if( rhop1.containsKey(key2) ){
					System.out.println("Find 3-hop answer: "+ X + " -> " + key1 + " -> " + key2 + " -> "+ Y);
				}
			}
		}
    }
}
