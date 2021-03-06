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