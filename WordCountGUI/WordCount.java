import java.awt.Button;
import java.awt.TextArea;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class WordCount implements ActionListener {
		// Cac frame va button
		public JFrame Frame;
		private JTextField Path;
		private Button BtnChonFile;
		private Button BtnRun;
		private Button BtnAddFile;
		private Button BtnResetFile;
		
		// Cac textArea va label hien thi ket qua 
		private TextArea lbDanhSachFile,lbDanhSachKQ;
		private JLabel lb1,lb3;
		
		// Cac bien khoi tao
		public String filename = null;
		public static Path[] input = new Path[10];
		public ArrayList<String> Multifile = new ArrayList<>();
		
		//khai bao
		public static Configuration c;
		public static String getFile = "";
		
		//ham main
		public static void main(String[] args) throws Exception {
			WordCount wordcount = new WordCount();
			wordcount.GUI(args);
		}
		
		public void GUI(String[] args)
		{
			// Duong dan 
			Path = new JTextField();
			Path.setBounds(200,30 , 200, 20);
			Path.setLayout(null);
			
			// button chon duong dan 
			BtnChonFile = new Button("Chon File");
			BtnChonFile.setBounds(410, 30, 80, 20);
			
			//button add nhieu file 
			BtnAddFile = new Button("Add a Hadoop input file");
			BtnAddFile.setBounds(200, 60, 50, 20);
			
			//button add reset file
			BtnResetFile = new Button("Reset");
			BtnResetFile.setBounds(270, 60, 50, 20);
			
			//button thong ke
			BtnRun = new Button("RUN MapReduce");
			BtnRun.setBounds(70,90, 100, 20);
			
			// hien thi 
			lb1 = new JLabel("Danh sach File:");
			lb1.setBounds(20, 110, 100, 20);
			lb1.setLayout(null);
			
			//Danh sach file can thong ke
			lbDanhSachFile = new TextArea("");
			lbDanhSachFile.setBounds(20, 130, 200, 250);
			//
			lb3 = new JLabel("Ket Qua");
			lb3.setBounds(250, 110, 80, 20);
			lb3.setLayout(null);
			// Danh sach Ket qua 
			lbDanhSachKQ = new TextArea("");
			lbDanhSachKQ.setBounds(250, 130, 360, 250);
			// Frame
			Frame = new JFrame("");
			Frame.setTitle("MapReduce WordCount GUI");
			Frame.setBounds(500, 200, 650, 440);
		  Frame.setLayout(null);
		  Frame.setVisible(true);
		  Frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		  
		  // them vao Frame
		  Frame.add(Path);
		  Frame.add(BtnChonFile);
		  Frame.add(BtnAddFile);
		  Frame.add(BtnResetFile);
		  Frame.add(BtnRun);
		  Frame.add(lb1);
		  Frame.add(lbDanhSachFile);
		  Frame.add(lb3);
		  Frame.add(lbDanhSachKQ);
		  
		  //Xu ly su kien click
		  BtnRun.addActionListener(new ActionListener() {
		 	  
				@Override
				public void actionPerformed(ActionEvent e) {
					//Reset ket qua 
					lbDanhSachKQ.setText("");
					
					// Kiem tra xem co chon file chua
					if(Multifile.size() == 0)
					{
						JOptionPane.showMessageDialog(Frame, "Chon file .txt de thuc hien Thong ke so tu khoa xuat hien trong cac documents");
						return;
					}
					
					// Xoa file output neu ton tai
					File file = new File("output");
					if (file.isDirectory())
					{
						 String[] children = file.list();
						 for (int i = 0; i < children.length; i++) {
							 File filechildren = new File(file, children[i]);
							 filechildren.delete();
						 }
						file.delete();
						System.out.println("Xoa file outputWC da co");
					}
					// Xoa file output neu ton tai
					File file2 = new File("//user");
					if (file2.isDirectory())
					{
						 String[] children = file.list();
						 for (int i = 0; i < children.length; i++) {
							 File filechildren = new File(file2, children[i]);
							 filechildren.delete();
						 }
						file2.delete();
						System.out.println("Xoa file outputWC da co");
					}
					// Xoa file output neu ton tai
					File file3 = new File("//tmp");
					if (file3.isDirectory())
					{
						 String[] children = file.list();
						 for (int i = 0; i < children.length; i++) {
							 File filechildren = new File(file3, children[i]);
							 filechildren.delete();
						 }
						file3.delete();
						System.out.println("Xoa file outputWC da co");
					}
					
					// Dua cac file txt vao 
					for(int i = 0;i<Multifile.size();i++)
					{
						input[i] = new Path(Multifile.get(i));
					}
					
					//Thuc hien Map-Reduce
					try 
					{
						MapReduce(input);
					} 
					catch (ClassNotFoundException e1) { e1.printStackTrace();} 
					catch (IOException e1) { e1.printStackTrace(); } 
					catch (InterruptedException e1) { e1.printStackTrace(); }
					
					// Doc file part-r-00000 vao cho hien thi ket qua 
					BufferedReader br = null;
					String kq = "";
			    try {  

					Path pt = new Path("hdfs://localhost:9000/output/part-r-00000");
					FileSystem fs = FileSystem.get(new Configuration());
					br = new BufferedReader(new InputStreamReader(fs.open(pt)));				 
					String textInALine = br.readLine();
					while (textInALine != null) {		
						kq += textInALine+"\n";
						textInALine = br.readLine();
			      }
						
			      lbDanhSachKQ.setText(kq.toString());

			    } catch (IOException e1) { e1.printStackTrace();
			    } finally {
			      try {
			        br.close();
			      } catch (IOException e1) { e1.printStackTrace(); }
			    }
				}				 
			});
		  
		  BtnAddFile.addActionListener(this);
		  BtnChonFile.addActionListener(this);
		  BtnResetFile.addActionListener(this);
		  
		}
		@Override
		public void actionPerformed(ActionEvent e) {
			if(e.getSource() == BtnChonFile)
			{
				JFileChooser fileChooser = new JFileChooser();
				FileNameExtensionFilter filter = new FileNameExtensionFilter(".txt", "All Files", ".txt");
				fileChooser.setFileFilter(filter);

				int i = fileChooser.showOpenDialog(null);
				if (i == JFileChooser.APPROVE_OPTION)
				  {
				   File file = fileChooser.getSelectedFile();
				   filename = file.getAbsolutePath();
					   try {
						Path.setText(filename);
					   } catch (Exception e2) {}   
				}
			}
			
			if(e.getSource() == BtnAddFile)
			{
				if(Path.getText().equals(""))
				{
					JOptionPane.showMessageDialog(Frame, "Xin chon file");
				}		
				else
				{
					Multifile.add(Path.getText());
					getFile += Path.getText()+"\n";
					Path.setText("");	
					lbDanhSachFile.setText(getFile);
				}
			}
		
			if(e.getSource() == BtnResetFile)
			{
				Multifile.removeAll(Multifile);
				lbDanhSachFile.setText("");
				getFile = "";
			}					
		}
		
		// Xu ly Map_Reduce
		public void MapReduce(Path[] input) throws IOException, ClassNotFoundException, InterruptedException
		{
			Configuration c = new Configuration();
			
			FileSystem fs = FileSystem.get(c);
			fs.delete(new Path("/output"), true);
			fs.delete(new Path("/user"), true);
			fs.delete(new Path("/tmp"), true);

			Path output = new Path("/output");
			Job j = new Job(c, "wordcount");
			j.setJarByClass(WordCount.class);
			j.setMapperClass(MapForWordCount.class);
			j.setReducerClass(ReduceForWordCount.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			for(int i = 0; i<Multifile.size(); i++)
			{
				FileInputFormat.addInputPath(j, input[i]);
			}
			FileOutputFormat.setOutputPath(j, output);
			j.waitForCompletion(true);
		}
			
		//Ham Map()
		public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
			public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException 
			{
				String line = value.toString();
				// Cat tu voi cac ki hieu sau ,.?!()\r\\n\"\\'
				String[] words = line.split("[ ,.?!()\r\\n\"\\']");
				for (String word : words) {
					Text outputKey = new Text(word.toUpperCase().trim());
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
			}
		}
		
		// Ham Reduce()
		public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
			public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException 
			{
				int sum = 0;
				for (IntWritable value : values) {
					sum += value.get();
				}
				con.write(word, new IntWritable(sum));
			}
		}
}
