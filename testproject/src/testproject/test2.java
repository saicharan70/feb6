package charan;


import org.apache.hadoop.filecache.DistributedCache;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class MR_dept_sal {
	
	public static class Map extends Mapper<Text,Text,Text,Text> {
		
		public void map(Text k,Text v,Context context) throws InterruptedException, IOException 
		{
		
			
			context.write(k,v);
		}
	}

	public static class Partition extends Partitioner<Text,IntWritable> {
		
		public int getPartition(Text k,IntWritable v,int p)
		{
			
			int sal = v.get();
			
			if(sal<1500)
			{
				return 0;
			}
			
			else 
				
				return 1;
				
				
			//String deptno = k.toString();
			
			//if(deptno.equals("10")||deptno.equals("20"))
			//{
		//		return 0;
		//	}
			
		//	else
		//		return 1;
		}
		
		
	}
	
	public static class Reduce extends Reducer<Text,IntWritable, Text,IntWritable> {
		
		public void reduce(Text k,Iterable<IntWritable> v, Context context)throws InterruptedException,IOException
		{
			
			Iterator<IntWritable> it = v.iterator();      
			
			int sumofsal = 0;
			
			while(it.hasNext())
			{
				sumofsal=sumofsal+it.next().get();
			}
			
			
			context.write(k, new IntWritable(sumofsal));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"job1");
		
		FileInputFormat.addInputPath(job, new Path("/home/cloudera/MR_input/dept.txt"));
		
	    //FileInputFormat.setMaxInputSplitSize(job, 300);
	    
		//job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap",5);
		
		
		
        //FileSystem fs = FileSystem.get(conf);
		
		//fs.delete(new Path("/user/cloudera/employee_s/emp_output7"),true);
		
		FileOutputFormat.setOutputPath(job,new Path("/home/cloudera/MR_input/emp_output40"));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);
	
		job.setMapperClass(Map.class);
		
		//job.setCombinerClass(Reduce.class);
		
		//job.setNumReduceTasks(2);
		
		//job.setPartitionerClass(Partition.class);
		
	    //job.setReducerClass(Reduce.class);
		
		job.setJarByClass(MR_dept_sal.class);
		
		job.waitForCompletion(true);
		

	}

}
