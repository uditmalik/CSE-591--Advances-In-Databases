package shared_nothing.spatialrange;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.*;


public class join_query {
	
public static double max(double n, double m)

{
	return (n>m)? n : m;
}
public static double min(double n, double m)

{
	return (n>m)? m : n;
}
	 
	public static void main(String[] args) 
	{
		/* creating spark context */
		SparkConf conf=new SparkConf().setAppName("JoinQuery").setMaster("spark://ubuntu:7077");
		JavaSparkContext sc=new JavaSparkContext(conf);
		sc.addJar("/home/master/Desktop/JQ.jar");
		/* Inputing the first input to be broadcasted */
		
			List<String> target = new ArrayList<String>();
			JavaRDD<String> in=sc.textFile("/home/master/Downloads/polygon2.txt");
			String[] input1;
			target=in.collect();
			input1=target.toArray(new String[0]);
			
			/* Using broadcast variable to broadcast the string array */
			Broadcast<String[]> br=sc.broadcast(input1); // Broadcasting the file as a string array
			final String[] broad;
			broad=br.value();
		
	   
	    /* 
		Creating a RDD of second input file 
		Passing the RDD to a mapToPair function and creating a Pair RDD as output of this transformation			
	    */
			
		JavaRDD<String> l2=sc.textFile("/home/master/Downloads/point1.txt");
		JavaPairRDD<String,String> j=l2.mapToPair(new PairFunction<String,String,String>()
		{
			
			public Tuple2<String, String> call(String data)
				{
				/* Splitting each line of the RDD passed  */
					String x = " : " ;
					String y=null;
					String parts[]=data.split(",");
				
					double x1=Double.parseDouble(parts[0]);
					double y1=Double.parseDouble(parts[1]);
					//double x2=Double.parseDouble(parts[2]);
					//double y2=Double.parseDouble(parts[3]);

					y=parts[0]+parts[1];
				
					for(String part: broad)
					{
						/* Splitting each line of broadcast variable in the loop */ 						
						String str[]=part.split(",");
					
						double a1=Double.parseDouble(str[0]);
						double b1=Double.parseDouble(str[1]);
						double a2=Double.parseDouble(str[2]);
						double b2=Double.parseDouble(str[3]);
					
					/* Condition whether one rectangle contains other rectangle or not */ 
					if((min(a1,a2) < x1) && (min(b1,b2) < y1) && (max(a1,a2) > x1) && (max(a1,a2) > y1))
						{
							x = x+ str[0] +","+str[1]+"\t";
						}
					
						
				}
					/* Returning output of map function as a tuple for each line of base RDD */
					return new Tuple2<String, String>(y,x); 
			}
		});
		
		try
			{
				/* Outputting the result RDD to Hdfs file system */
				j.repartition(1).collect();
			}
		
		catch(Exception e)
			{
				e.printStackTrace();
			}
		
		
		/* Closing the spark context */
		sc.close();
	}
}
