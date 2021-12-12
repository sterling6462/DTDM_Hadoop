import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.HashMap;
 
class MatrixMultiplyMapper extends Mapper <LongWritable, Text, Text, Text> {
   public void map(LongWritable key, Text value, Context outputMap) throws IOException, InterruptedException {
  
          Configuration conf = outputMap.getConfiguration();
          int m = Integer.parseInt(conf.get("m"));
          int p = Integer.parseInt(conf.get("p"));
          String line = value.toString();
         
          // (M, i, j, Mij);
          String[] indicesAndValue = line.split(",");
          Text outputKey = new Text();
          Text outputValue = new Text();
         
          if (indicesAndValue[0].equals("M")) {
                 for (int k = 0; k < p; k++) {
                        outputKey.set(indicesAndValue[1] + "," + k);
                        // outputKey.set(i,k);
                        outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2] + "," + indicesAndValue[3]);
                        // outputValue.set(M,j,Mij);
                        outputMap.write(outputKey, outputValue);
                 }
          } else {
                 // (N, j, k, Njk);
                 for (int i = 0; i < m; i++) {
                        outputKey.set(i + "," + indicesAndValue[2]);
                        outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]);
                        outputMap.write(outputKey, outputValue);
                 }
          }
   }
  }//class MatrixMultiplyMapper
 
  class MatrixMultiplyReduce extends Reducer <Text, Text, Text, Text> {
   public void reduce(Text key, Iterable<Text> values, Context outputReduce) throws IOException, InterruptedException {
          String[] value;
          //key=(i,k),
          //Values = [(M/N,j,V/W),..]
          HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
          HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
          for (Text val : values) {
                 value = val.toString().split(",");
                 if (value[0].equals("M")) {
                        hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                 } else {
                        hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                 }
          }
          int n = Integer.parseInt(outputReduce.getConfiguration().get("n"));
          float result = 0.0f;
          float m_ij;
          float n_jk;
          for (int j = 0; j < n; j++) {
                 m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                 n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                 result += m_ij * n_jk;
          }
          if (result != 0.0f) {
                 outputReduce.write(null, new Text(key.toString() + "," + Float.toString(result)));
          }
   }
  }//class MatrixMultiplyReduce
 
public class MatrixMultiply {
   static Configuration conf;
   public int run(String pathin, String pathout) throws Exception {
                
          Job job = new Job(conf, "MatrixMultiply");
          job.setJarByClass(MatrixMultiply.class);
 
          job.setMapperClass(MatrixMultiplyMapper.class);
          job.setReducerClass(MatrixMultiplyReduce.class);
       
          job.setInputFormatClass(TextInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);   
         
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
                       
          FileInputFormat.addInputPath(job, new Path(pathin));
          FileOutputFormat.setOutputPath(job, new Path(pathout));
 
          boolean success = job.waitForCompletion(true);
          return success ? 0 : -1;
    }
  
    public static void main(String[] args) throws Exception {
         
          conf = new Configuration();
          // M is an m-by-n matrix; N is an n-by-p matrix.
          conf.set("m", "1000");
          conf.set("n", "100");
          conf.set("p", "1000");
         
          FileSystem fs = FileSystem.get(conf);
          //Delete output folder if it already exists
          fs.delete(new Path(args[1]), true);
                       
          MatrixMultiply runner = new MatrixMultiply();
          runner.run(args[0], args[1]);
       
    }//main
}//MatrixMultiply
 
