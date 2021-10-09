package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @description:
 * @author: 龙辉辉
 * @create: 2021-09-26 22:22
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //1.直接给定
        conf.setInt("top.n",20);

        // conf.setInt("top.n",Integer.parseInt(args[0]));
        job.setJarByClass(Main.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(wordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(wordReducer.class);
        job.setReducerClass(wordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/Users/qishi/Desktop/Mapreducer/inputData/task1-input1.txt"));
        FileInputFormat.addInputPath(job, new Path("/Users/qishi/Desktop/Mapreducer/inputData/task1-input2.txt"));
       // FileInputFormat.addInputPath(job, new Path("/Users/qishi/Desktop/Mapreducer/inputData/stopwords.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/qishi/Desktop/Mapreducer/output/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
