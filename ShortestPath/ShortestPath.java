import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Partitioner;
 
class ShortestPathMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
  private static final String DISCOVERED                 = "DISCOVERED";
  private static final String UNDISCOVERED               = "UNDISCOVERED";
  private static final String INFINITY                   = "INFINITY";
  private static final int    MINIMUM_SOURCE_NODE_TOKENS = 3;
  private static final int    MAXIMUM_SOURCE_NODE_TOKENS = 4;
  public static final int     INDEX_SOURCE_NODE_NUMBER   = 0;
  public static final int     INDEX_SOURCE_NODE_WEIGHT   = 1;
  private static final int    INDEX_SOURCE_NODE_STATUS   = 2;
  private static final int    INDEX_SOURCE_NODE_PATH     = 3;
 
  private Text                emptyText                  = new Text();
 
  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> outputMapper, Reporter reporter) throws IOException
  {
    String[] sourceNodeDetails = key.toString().substring(1, key.toString().length() - 1).split(",");
    if (sourceNodeDetails.length >= MINIMUM_SOURCE_NODE_TOKENS
        && !sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT].equalsIgnoreCase(INFINITY)
        && !sourceNodeDetails[INDEX_SOURCE_NODE_STATUS].equalsIgnoreCase(DISCOVERED))
    {
      String currentPath = "";
      if (sourceNodeDetails.length == MAXIMUM_SOURCE_NODE_TOKENS)
      {
        currentPath = sourceNodeDetails[INDEX_SOURCE_NODE_PATH];
      }
      currentPath += (currentPath.length() == 0 ? "" : "-") + sourceNodeDetails[INDEX_SOURCE_NODE_NUMBER];
 
      outputMapper.collect(new Text("{" + sourceNodeDetails[INDEX_SOURCE_NODE_NUMBER] + "," + sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT] + "," + DISCOVERED + "," + currentPath + "}"), value);
           System.out.println(">>> MAP1: Key = [" + new Text("{" + sourceNodeDetails[INDEX_SOURCE_NODE_NUMBER] + "," + sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT] + "," + DISCOVERED + "," + currentPath + "}") + "] Value = [" + value +"]");
 
      if (value.toString().trim().length() > 0)
      {
        String[] tokens = value.toString().trim().split("\t");
        String[][] adjacentNodeDetails = new String[tokens.length][2];
        for (int index = 0; index < tokens.length; index++)
        {
          adjacentNodeDetails[index] = tokens[index].substring(1, tokens[index].length() - 1).split(",");
        }
 
        int sourceNodeWeight = Integer.parseInt(sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT]);
        for (int index = 0; index < tokens.length; index++)
        {
          int number = sourceNodeWeight + Integer.parseInt(adjacentNodeDetails[index][1]);
 
          outputMapper.collect(new Text("{" + adjacentNodeDetails[index][0] + "," + Integer.toString(number) + "," + UNDISCOVERED + "," + currentPath + "}"), emptyText);
 
          reporter.incrCounter(ShortestPath.CUSTOM_COUNTERS, ShortestPath.NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED, 1);
                   System.out.println(">>> MAP2: Key = [" + new Text("{" + adjacentNodeDetails[index][0] + "," + Integer.toString(number) + "," + UNDISCOVERED + "," + currentPath + "}") + "] Value = [" + emptyText +"]");
        }
      }
    }
    else
    {
      outputMapper.collect(key, value);
           System.out.println(">>> MAP3: Key = [" + key + "] Value = [" + value +"]");
    }
  }
}//ShortestPathMapper
 
class ShortestPathReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputReducer, Reporter reporter)throws IOException
  {
    int index = 0;
    String newValue = "";
    while (values.hasNext())
    {
      if (index > 0)
      {
        newValue += "\t";
      }
      newValue += values.next().toString();
    }
 
    outputReducer.collect(key, new Text(newValue));
         System.out.println(">>> REDUCE: Key = [" + key + "], Value = [" + new Text(newValue) + "]");
        
  }
}//ShortestPathReducer
 
class ShortestPathPartitioner implements Partitioner<Text, Text>
{
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    String compositeKey = key.toString();
 
    String[] nodeDetails = compositeKey.substring(1, compositeKey.length() - 1).split(",");
 
    int returnValue = (nodeDetails[ShortestPathMapper.INDEX_SOURCE_NODE_NUMBER].hashCode() & Integer.MAX_VALUE)% numReduceTasks;
 
    return returnValue;
  }
 
  public void configure(JobConf job) { }
}//ShortestPathPartitioner
        
         public class ShortestPath extends Configured implements Tool
         {
           public static final String CUSTOM_COUNTERS                              = "Custom Counters";
           public static final String NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED = "NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED";
 
           @Override
           public int run(String[] args) throws Exception
           {
 
             int iteration = 0;
             long toBeProcessed = 0;
             boolean finalRun = false;
             String inputPath = args[0];
             String outputPath =  args[1];
             do
             {
               JobConf conf = new JobConf(getConf(), ShortestPath.class);
               conf.setJobName(this.getClass().getName());
 
               FileInputFormat.setInputPaths(conf, new Path(inputPath));
               FileOutputFormat.setOutputPath(conf, new Path(finalRun ? outputPath : outputPath + "-" + iteration));
        
               conf.setInputFormat(KeyValueTextInputFormat.class);
 
               conf.setMapperClass(ShortestPathMapper.class);
               conf.setOutputKeyComparatorClass(ShortestPathOutputKeyComparator.class);
               conf.setOutputValueGroupingComparator(ShortestPathOutputValueGroupingComparator.class);
               conf.setPartitionerClass(ShortestPathPartitioner.class);
               conf.setReducerClass(ShortestPathReducer.class);
          
               if (finalRun)
               {
                 conf.setNumReduceTasks(1);
               }
 
               conf.setMapOutputKeyClass(Text.class);
               conf.setMapOutputValueClass(Text.class);
 
               conf.setOutputKeyClass(Text.class);
               conf.setOutputValueClass(Text.class);
 
               RunningJob job = JobClient.runJob(conf);
 
               inputPath = outputPath + "-" + iteration;
               iteration++;
               toBeProcessed = job.getCounters().findCounter(CUSTOM_COUNTERS, NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED).getValue();
            
               finalRun = (toBeProcessed == 0 && !finalRun);
             }
             while (toBeProcessed > 0 || finalRun);
 
             return 0;
           }
 
           public static void main(String[] args) throws Exception
           {
             int exitCode = ToolRunner.run(new ShortestPath(), args);
             System.out.println("Ket thuc!!!");
             System.exit(exitCode);
           }
         }
 
 
