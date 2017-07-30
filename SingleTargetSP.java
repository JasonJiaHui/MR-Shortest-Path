package comp9313.ass2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Comp9313 Ass2
 * Date: 13/04/2017  Author: Jia Hui ID:z5077824
 * 
 * This assignment is about how to find the shortest path in a graph by using map reduce. Well, it's almost same as Dijstra algorithm.
 * I'd like to outline this ass2 as three major parts:
 * 		==>Part1: 
 * 			Initialise input file to the format of:  s --> 0 | n1:10, n2:5. I use the hadoop io to format input file and write as output file
 * 			Note that, there is a trick here,although this ass is different from single-source shortest path, it can be very easy to modify.
 * 			Eg:
 * 				input file:
 * 							0	0	1	10.0
 * 							1	1	2	3.0			assume query id 1, just reverse the arrow.
 * 							2	1	3	1.0					==>  0 --> INF | 2:5, 4:2
 * 							3	2	1	3.0						 1 --> 0   | 0:10, 2:3
 * 							...............						 2 --> INF | 1:3.0
 * 																 .....................
 * 
 * 		==>Part2:
 * 			COre part of ass, iterate the map reduce jobs with the help of counter to decide when can be terminated
 * 			
 * 			Mapper:
 * 				Here I maintain two hashmap to store the distance info. One is the keyMap, and the other is valueMap
 * 				Note that my valueMap only store the min distance, I mainly use it to compared with keyMap to decide whether it has update...
 * 			
 * 			Eg:
 * 				input file:
 * 						3-->INF|1:1.0,2:9.0,4:6.0
 *						2-->INF|1:3.0				keyMap: {0:INF, 1:0, 2:INF, 3:INF, 4:INF}    
 *						1-->0.0|0:10.0,2:3.0		==> valueMap: {0:10, 1:0, 2:3, 4:INF, 5:INF} 
 *						0-->INF|2:5.0,4:2.0			emit: (3, INF), (4, INF), (2, INF), (4, INF), (3, <1:1.0, 2:9.0, 4:6.0>)		
 *						4->INF|2:2.0,3:4.0			(2, INF), (1, INF), (2, <1:3.0>)
 * 													..............................
 *  				*** I diff keyMap and valueMap to set counter, if not same +1, otherwise 0
 *  				*** after each iteration, I check the counter value in main function, if counter = 0, ==> converge....
 *  
 * 			Reducer:
 * 				Here, I select the min distance to update, and concatenate with adjacent table.
 * 				Eg:
 * 					received: (0, (10, INF, <2:5.0, 4:2.0>))  			==> 0	-->10.0|2:5.0,4:2.0
 * 							  (1, (INF, INF, 0, <0:10.0, 2:3.0>))		==> 1	-->0.0|0:10.0,2:3.0 
 * 							.....................							...................
 * 			

 * 		==>Part3:
 * 			
 * 			This part is mainly format the output result and save it into output folder by using the other map reduce.
 * 
 * 			Eg:
 * 				input file:
 * 			
 * 					0	-->10.0|2:5.0,4:2.0									1	0	10.0
 *					1	-->0.0|0:10.0,2:3.0			write to output			1	1	0.0
 *					2	-->3.0|1:3.0   						==>> 			1	2	3.0	
 *					3	-->16.0|1:1.0,2:9.0,4:6.0							1	3	16.0
 *					4	-->12.0|2:2.0,3:4.0									1	4	12.0
 *
 */



public class SingleTargetSP {	
	
	// init counter
	enum Helper{
		CHNANGED
	}
	
	
    public static String OUT = "output";
    public static String IN = "input";

    /*
     *  * ==>Part1: 
	 * 			Initialise input file to the format of:  s --> 0 | n1:10, n2:5. I use the hadoop io to format input file and write as output file
	 * 			Note that, there is a trick here,although this ass is different from single-source shortest path, it can be very easy to modify.
	 * 			Eg:
	 * 				input file:
	 * 							0	0	1	10.0
	 * 							1	1	2	3.0			assume query id 1, just reverse the arrow.
	 * 							2	1	3	1.0					==>  0 --> INF | 2:5, 4:2
	 * 							3	2	1	3.0						 1 --> 0   | 0:10, 2:3
	 * 							...............						 2 --> INF | 1:3.0
	 * 																 .....................
     * 
     * 
     */
  
    
    public static class InitMapper extends Mapper<Object, Text, IntWritable, Text> {
    	
    	Map<String, String> myMap = new HashMap<String, String>();
    	
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    		StringTokenizer itr = new StringTokenizer(value.toString(), " ");
    	
    		ArrayList<String> list = new ArrayList<String>();
    		while(itr.hasMoreTokens()){
    			list.add(itr.nextToken());
    		}
    		
    		String id, adjTable = "";
    		id = list.get(2);
			if(myMap.containsKey(id)){
				adjTable = list.get(1) + ":" + list.get(3);
				myMap.put(id, myMap.get(id) + "," + adjTable);
			}else{
				adjTable = list.get(1) + ":" + list.get(3);
				myMap.put(id, adjTable);
			}
    		
    	}
    	
    	public void cleanup(Context context) throws IOException, InterruptedException{
    		
    		String id = "";
    		Set<String> set =  myMap.keySet();
    		Iterator<String> it = set.iterator();	
    		String Qid = context.getConfiguration().get("queryId");
    		
    		while(it.hasNext()){
    			id = it.next();
    			if(Qid.compareTo(id) == 0){
    				context.write(new IntWritable(Integer.parseInt(id)), new Text("-->" + "0.0" + "|" + myMap.get(id)));
    			}else{
    				context.write(new IntWritable(Integer.parseInt(id)), new Text("-->" + "INF" + "|" + myMap.get(id)));
    			}
    		}
    	}
    }
    
    public static class InitReducer extends Reducer<IntWritable, Text, Text, Text> {
    	
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    		String res = "";
    		for(Text val : values){
    			res += val;
    		}

    		context.write(new Text(key.get()+""), new Text(res));
    	}
    }
    	

    /*
     *  * 	Mapper:
 	 * 			Here I maintain two hashmap to store the distance info. One is the keyMap, and the other is valueMap
 	 * 			Note that my valueMap only store the min distance, I mainly use it to compared with keyMap to decide whether it has update...
 	 * 			
 	 * 			Eg:
 	 * 				input file:
 	 * 						3-->INF|1:1.0,2:9.0,4:6.0
 	 *						2-->INF|1:3.0				keyMap: {0:INF, 1:0, 2:INF, 3:INF, 4:INF}    
 	 *						1-->0.0|0:10.0,2:3.0		==> valueMap: {0:10, 1:0, 2:3, 4:INF, 5:INF} 
 	 *						0-->INF|2:5.0,4:2.0			emit: (3, INF), (4, INF), (2, INF), (4, INF), (3, <1:1.0, 2:9.0, 4:6.0>)		
 	 *						4->INF|2:2.0,3:4.0			(2, INF), (1, INF), (2, <1:3.0>)
 	 * 													..............................
 	 *  				*** I diff keyMap and valueMap to set counter, if not same +1, otherwise 0
 	 *  				*** after each iteration, I check the counter value in main function, if counter = 0, ==> converge....
      *  
      *  
      */
    
    public static class STMapper extends Mapper<Object, Text, Text, Text> {
    	
    	Map<String, String> keyMap = new HashMap<String, String>();
    	Map<String, String> valueMap = new HashMap<String, String>();

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        	StringTokenizer itr = new StringTokenizer(value.toString(),"|");
        	
        	String id = "";
        	String pair = "";
        	String distance = "";
        	String adjTable = "";
      
        	
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if(token.contains("-->")){
					id = token.split("-->")[0].trim();
					distance = token.split("-->")[1];
					
					keyMap.put(id, distance);
					
					if(!valueMap.containsKey(id)){
						valueMap.put(id, distance);
					}else{
						String d = valueMap.get(id);
						if("INF".compareTo(distance) != 0){
							if("INF".compareTo(d) == 0){ 
								valueMap.put(id, distance);
							}else if(Double.parseDouble(d) > Double.parseDouble(distance)){
								valueMap.put(id, distance);
							}
						}
					}
					
					context.write(new Text(id), new Text(distance));
				}else if(token.compareTo("") != 0){
					adjTable = token;
					if(token.contains(",")){
						String arr[] = token.split(",");
						for(int i = 0; i < arr.length; i++){
							pair = arr[i];
							String eles[] = pair.split(":");
							if("INF".compareTo(distance) == 0){
								context.write(new Text(eles[0]), new Text("INF"));
								
								if(!valueMap.containsKey(eles[0])){
									valueMap.put(eles[0], "INF");
								}
								
								
							}else{
								double dDistance = Double.parseDouble(distance);
								double len = Double.parseDouble(eles[1]);
								context.write(new Text(eles[0]), new Text("" + (dDistance + len)));
								
								if(!valueMap.containsKey(eles[0])){
									valueMap.put(eles[0], ""+(dDistance + len));
								}else{
									String currentDistance = valueMap.get(eles[0]);
									if("INF".compareTo(currentDistance) == 0){
										valueMap.put(eles[0], ""+(dDistance + len));
									}else{
										double dis = Double.parseDouble(currentDistance);
										if(dis > (dDistance + len)){
											valueMap.put(eles[0], ""+(dDistance + len));
										}
									}
								}
							}
						}
						
					}else{
						pair = token;
						String eles[] = pair.split(":");
						
						if("INF".compareTo(distance) == 0){
							context.write(new Text(eles[0]), new Text("INF"));
							
							if(!valueMap.containsKey(eles[0])){
								valueMap.put(eles[0], "INF");
							}
							
						}else{
							double dDistance = Double.parseDouble(distance);
							double len = Double.parseDouble(eles[1]);
							
							double newDis = dDistance + len;
//							System.out.println("Dis: " + dDistance + " len: " + len + "  newDIs: " + newDis);
							
							if(!valueMap.containsKey(eles[0])){
								valueMap.put(eles[0], "" + newDis);
							}else{
								String currentDistance = valueMap.get(eles[0]);
								if("INF".compareTo(currentDistance) == 0){
									valueMap.put(eles[0], ""+newDis);
								}else{
									double dis = Double.parseDouble(currentDistance);
									if(dis > newDis){
										valueMap.put(eles[0], ""+newDis);
									}
								}
							}
							context.write(new Text(eles[0]), new Text("" + newDis));
						}
					}
				}
		
			}
		
			context.write(new Text(id), new Text(adjTable));
        }
        
        
		public void cleanup(Context context) throws IOException, InterruptedException{
		
			Set<Entry<String, String> > sets = keyMap.entrySet();
			for(Entry<String, String> entry : sets){
				String key = entry.getKey();
				if(entry.getValue().compareTo(valueMap.get(key)) != 0){
					context.getCounter(Helper.CHNANGED).increment(1);
				}
				
			}
			
//			System.out.println("Map counter===>>>> " + context.getCounter(Helper.CHNANGED).getValue());
		}
			
	}


    /*
     *  * Here, I select the min distance to update, and concatenate with adjacent table.
     * 				Eg:
     * 					received: (0, (10, INF, <2:5.0, 4:2.0>))  			==> 0	-->10.0|2:5.0,4:2.0
     * 							  (1, (INF, INF, 0, <0:10.0, 2:3.0>))		==> 1	-->0.0|0:10.0,2:3.0 
     * 							.....................							...................
     * 
     */
    public static class STReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  
        	String adjTable = "";
        	Double min = Double.MAX_VALUE;
        	for(Text value : values){
        		if(value.toString().contains(":")){
        			adjTable = value.toString();
        		}else{
        			if("INF".compareTo(value.toString()) == 0) continue;
        			if("".equals(value.toString())) continue;
        			double val = Double.parseDouble(value.toString());
        			if(min > val){
        				min = val;
        			}
        		}
        		
        	}

        	if(min == Double.MAX_VALUE){
        		context.write(key, new Text("-->INF|" + adjTable));
        	}else{
        		context.write(key, new Text("-->" + min + "|" + adjTable));
        	}
        }
    }

    /*
     *  This part is mainly format the output result and save it into output folder by using the other map reduce.
	 * 
	 * 			Eg:
	 * 				input file:
	 * 			
	 * 					0	-->10.0|2:5.0,4:2.0									1	0	10.0
	 *					1	-->0.0|0:10.0,2:3.0			write to output			1	1	0.0
	 *					2	-->3.0|1:3.0   						==>> 			1	2	3.0	
	 *					3	-->16.0|1:1.0,2:9.0,4:6.0							1	3	16.0
	 *					4	-->12.0|2:2.0,3:4.0									1	4	12.0
     * 
     */
	
    public static class FinalMapper extends Mapper<Object, Text, IntWritable, Text> {
    	
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		StringTokenizer itr = new StringTokenizer(value.toString(),"|");
    		
    		String id = "";
    		String distance = "";
    		String queryId = context.getConfiguration().get("queryId");
    		
    		while (itr.hasMoreTokens()) {
    		
				String token = itr.nextToken();
				if(token.contains("-->")){
					id = token.split("-->")[0].trim();
					distance = token.split("-->")[1];
				
					if("INF".compareTo(distance) != 0){
						context.write(new IntWritable(Integer.parseInt(id)), new Text(distance));
					}
					// deal with 
					if("INF".equals(distance) && id.equals(queryId)){
//						System.out.println("*****************************************");
						context.write(new IntWritable(Integer.parseInt(id)), new Text("0.0"));
					}
				}
    			
    		}
    	}
    	
    }
    
    public static class FinalReducer extends Reducer<IntWritable, Text, Text, Text> {

    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    		String queryId = context.getConfiguration().get("queryId");
   
    		int counter = 0;
    		
    		for(Text val : values){
    			counter++;
    			context.write(new Text(queryId), new Text(key + "\t" + val.toString()));
    		}
    		
//    		if(counter == 0){
//    			context.write(new Text(queryId), new Text(key + "\t" + "0.0"));
//    		}
    	}
    }
	
	// Main Driver
	public static void main(String[] args) throws Exception {
		
		IN = args[0];
		OUT = args[1];
		
		Configuration conf = new Configuration();
		
		conf.set("queryId", args[2]);

		String input = IN;
		String output = OUT + System.nanoTime();

		Job job = null;
		
		
		// Initialise input file
	    job = Job.getInstance(conf, "Graph");
	    job.setJarByClass(SingleTargetSP.class);
	    job.setMapperClass(InitMapper.class);   
	    job.setReducerClass(InitReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		
		boolean isdone = false;
		// Iterate the map reduce job based on the change counter
		while(isdone == false){
			
		    job = Job.getInstance(conf, "Graph");
		    job.setJarByClass(SingleTargetSP.class);
		    job.setMapperClass(STMapper.class);   
		    job.setReducerClass(STReducer.class);
		    job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			input = output;
			output = OUT + System.nanoTime();
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			job.waitForCompletion(true);
			
			// check the changed counter
			if(job.getCounters().findCounter(Helper.CHNANGED).getValue() == 0){
				isdone = true;
			}
		}
		
		
		// format the final result
	    job = Job.getInstance(conf, "Graph");
	    job.setJarByClass(SingleTargetSP.class);
	    job.setMapperClass(FinalMapper.class);   
  
	    job.setReducerClass(FinalReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		    
		input = output;
		output = OUT;
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);	

	}

}
