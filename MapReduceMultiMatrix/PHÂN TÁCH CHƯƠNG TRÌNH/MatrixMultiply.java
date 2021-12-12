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
 
