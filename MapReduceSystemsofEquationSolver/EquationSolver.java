import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
 
public class EquationSolver {
   
    public static class EquationSolverMapper extends Mapper<Object, Text, Text, IntWritable>{
    public static Text text = new Text();
         
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                   String []input = value.toString().split("\n");
                  
                   int row = input.length;
                   int column = input[0].length();
                  
                   int [][]myArray = new int[row][column];
                
                   for (int i=0; i<myArray.length; i++) {
                    String[] line = input[i].trim().split(" ");
                    for (int j=0; j<line.length; j++) {
                       myArray[i][j] = Integer.parseInt(line[j]);
                    }
                 }
                   System.out.println(myArray);
                   text.set("det");
                   int[] d = new int[row];
               for(int i = 0; i< row; i++ ) d[i] = myArray[i][column-1];
               int[][] matrix = copy(myArray);
               int res = det(matrix);
              
               IntWritable det = new IntWritable(res);
                context.write(text, det);
               int[] dt = new int[row];
               for(int j = 0; j < row; j++ ){
                   int[][] matrix1 = copy(myArray);
                   for(int i = 0; i< row; i++ )
                       matrix1[i][j] = d[i];
                   dt[j] = det(matrix1);
                   text.set("det" + (j+1));
                   IntWritable valueDet = new IntWritable(dt[j]);
                   context.write(text, valueDet);
               }
           }
    }    
 
    static int[][] copy(int[][] arr ){
        int[][] newArr  = new int[arr.length][arr.length];
        for (int i = 0; i < arr.length; i++) {
            for(int j = 0; j< arr.length; j++ ){
                newArr[i][j] = arr[i][j];
            }
        }
        return newArr;
    }
   
    static int det(int[][] arr){ //Tinh & tra ve dinh thuc
 
        if(arr.length == 1) return arr[0][0];
        if(arr.length  == 2 ) return arr[0][0]*arr[1][1] - arr[0][1]*arr[1][0];
        else{
           
            int res = 0;
            for(int k = 0; k< arr.length; k++ ){
                int[][] smaller = new int[arr.length-1][arr.length-1];
                for(int i = 0; i< arr.length; i++ ){
                    for(int j = 1;  j< arr.length;  j++ ){
                        if(i< k) smaller[i][j-1] = arr[i][j];
                        else if(i > k) smaller[i-1][j-1] = arr[i][j];
                    }
                }
                int s = -1;
                if(k%2 == 0 ) s =  1;
                res+= arr[k][0]*s*det(smaller);
            }
            return res;
        }
    }
   
public static class EquationSolverReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
         
           public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                 int[] arrDet = null;
                 for (IntWritable val : values) {
                       int i = 0;
                       arrDet[i] = val.get();
                       i++;
                 }
                 
                 for (int i = 1; i < arrDet.length; i++) {
                       key.set("x" + i);
                       IntWritable result = new IntWritable(arrDet[i]/arrDet[0]);
                       context.write(key, result);
                 }
          }//reduce
    }//class EquationSolverReduce
 
     
    public static void main(String[] args) throws Exception {
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "Systems of Equations Solver");
         
          job.setJarByClass(EquationSolver.class);
        job.setMapperClass(EquationSolverMapper.class);
        job.setCombinerClass(EquationSolverReduce.class);
        job.setReducerClass(EquationSolverReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         
          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}//EquationSolver
