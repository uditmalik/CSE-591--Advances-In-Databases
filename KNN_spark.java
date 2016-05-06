package shared_nothing.spatialrange;


import scala.Tuple2; 
import org.apache.spark.broadcast.Broadcast; 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.api.java.function.PairFunction; 
 
import java.util.Map; 
import java.util.HashMap; 
import java.util.SortedMap; 
import java.util.TreeMap; 
import java.util.List; 
import java.util.ArrayList; 
import com.google.common.base.Splitter; 

public class KNN { 
    
   
    static List<Double> splitOnToListOfDouble(String str, String delimiter) { 
       Splitter splitter = Splitter.on(delimiter).trimResults(); 
       Iterable<String> tokens = splitter.split(str); 
       if (tokens == null) { 
          return null; 
       } 
       List<Double> list = new ArrayList<Double>(); 
       for (String token: tokens) { 
         double data = Double.parseDouble(token); 
         list.add(data); 
       } 
       return list; 
    }    
    
   
   static double calculateDistance(String rAsString, String sAsString, int d) { 
      List<Double> r = splitOnToListOfDouble(rAsString, ","); 
      List<Double> s = splitOnToListOfDouble(sAsString, ","); 
  
      // d is the number of dimensions in the vector  
      if (r.size() != d) { 
         return Double.NaN; 
      } 
      if (s.size() != d) { 
         return Double.NaN; 
      }       
       
      // here we have (r.size() == s.size() == d)  
      double sum = 0.0; 
      for (int i = 0; i < d; i++) { 
        double difference = r.get(i) - s.get(i); 
        sum += difference * difference; 
      } 
      return Math.sqrt(sum); 
   } 
 
   static SortedMap<Double, String> findNearestK(Iterable<Tuple2<Double,String>> neighbors, int k) { 
       // keep only k-nearest-neighbors 
       SortedMap<Double, String>  nearestK = new TreeMap<Double, String>(); 
       for (Tuple2<Double,String> neighbor : neighbors) { 
          Double distance = neighbor._1; 
          String classificationID =  neighbor._2; 
          nearestK.put(distance, classificationID); 
          // keep only k-nearest-neighbors 
          if (nearestK.size() > k) { 
             // remove the last highest distance neighbor from nearestK 
             nearestK.remove(nearestK.lastKey()); 
          }       
       } 
       return nearestK; 
   } 
  
   static Map<String, Integer> buildClassificationCount(Map<Double, String> nearestK) { 
       Map<String, Integer> majority = new HashMap<String, Integer>(); 
       for (Map.Entry<Double, String> entry : nearestK.entrySet()) { 
          String classificationID = entry.getValue(); 
          Integer count = majority.get(classificationID); 
          if (count == null){ 
             majority.put(classificationID, 1); 
          } 
          else { 
             majority.put(classificationID, count+1); 
          } 
       }  
       return majority; 
   }         
 
   static String classifyByMajority(Map<String, Integer> majority) { 
     int votes = 0; 
     String selectedClassification = null; 
     for (Map.Entry<String, Integer> entry : majority.entrySet()) { 
        if (selectedClassification == null) { 
            selectedClassification = entry.getKey(); 
            votes = entry.getValue(); 
        } 
        else { 
            int count = entry.getValue(); 
            if (count > votes) { 
                selectedClassification = entry.getKey(); 
                votes = count; 
            } 
        } 
     } 
     return selectedClassification; 
   } 
    
   public static void main(String[] args) throws Exception { 
    // STEP-1: Handle input parameters 
    if (args.length < 5) { 
      System.err.println("Usage: kNN <k-knn> <d-dimension> <R> <S> <yarn's-resource-manager-host"); 
      System.exit(1); 
    } 
    Integer k = Integer.valueOf(args[0]); // k for kNN 
    Integer d = Integer.valueOf(args[1]); // d-dimension 
    String datasetR = args[2]; 
    String datasetS = args[3]; 
    String yarnResourceManager = args[4]; 
     
 
    SparkConf conf=new SparkConf().setAppName("JoinQuery").setMaster("spark://ubuntu:7077");
	JavaSparkContext sc=new JavaSparkContext(conf);
	sc.addJar("/home/master/Desktop/JQ.jar"); 
 
    
    final Broadcast<Integer> broadcastK = sc.broadcast(k); 
    final Broadcast<Integer> broadcastD = sc.broadcast(d); 
 
    // STEP-4: Create RDDs for query and training datasets 
    JavaRDD<String> R = sc.textFile(datasetR, 1); 
    R.saveAsTextFile("/output/R");   
    JavaRDD<String> S = sc.textFile(datasetS, 1); 
    S.saveAsTextFile("/output/S"); 
 
    
    JavaPairRDD<String,String> cart = R.cartesian(S); 
    cart.saveAsTextFile("/output/cart"); 
     
     
    JavaPairRDD<String,Tuple2<Double,String>> knnMapped = 
            //                              input                  K       V 
            cart.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<Double,String>>() { 
      public Tuple2<String,Tuple2<Double,String>> call(Tuple2<String,String> cartRecord) { 
        String rRecord = cartRecord._1; 
        String sRecord = cartRecord._2; 
        String[] rTokens = rRecord.split(";");  
        String rRecordID = rTokens[0]; 
        String r = rTokens[1]; //  r.1, r.2, ..., r.d 
        String[] sTokens = sRecord.split(";");  
        // sTokens[0] = s.recordID 
        String sClassificationID = sTokens[1];  
        String s = sTokens[2]; // s.1, s.2, ..., s.d 
        Integer d = broadcastD.value(); 
        double distance = calculateDistance(r, s, d); 
        String K = rRecordID; //  r.recordID 
        Tuple2<Double,String> V = new Tuple2<Double,String>(distance, sClassificationID); 
        return new Tuple2<String,Tuple2<Double,String>>(K, V); 
      } 
    }); 
    knnMapped.saveAsTextFile("/output/knnMapped");     
 
    //Group distances by r in R 
    // now group the results by r.recordID and then find the k-nearest-neigbors. 
    JavaPairRDD<String, Iterable<Tuple2<Double,String>>> knnGrouped = knnMapped.groupByKey(); 
     
    // find the k-nearest-neigbors and classify r 
    // mapValues[U](f: (V) => U): JavaPairRDD[K, U] 
    // Pass each value in the key-value pair RDD through a  
    // map function without changing the keys; 
    // this also retains the original RDD's partitioning. 
    // Generate (K,V) pairs where K=r.recordID, V = classificationID 
    JavaPairRDD<String, String> knnOutput = 
        knnGrouped.mapValues(new Function<Iterable<Tuple2<Double,String>>, // input 
                                          String                           // output (classification) 
                                      >() { 
      public String call(Iterable<Tuple2<Double,String>> neighbors) { 
          Integer k = broadcastK.value(); 
          // keep only k-nearest-neighbors 
          SortedMap<Double, String> nearestK = findNearestK(neighbors, k); 
           
          
          Map<String, Integer> majority = buildClassificationCount(nearestK); 
             
          // find a classificationID with majority of vote 
          String selectedClassification = classifyByMajority(majority); 
          return selectedClassification; 
      } 
    });    
    knnOutput.saveAsTextFile("/output/knnOutput"); 
 
    System.exit(0); 
  } 
}


