import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
/**
 * @description:
 * @author: 龙辉辉
 * @create: 2021-09-21 21:17
 */
public class TopkCommonWords {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //1.直接给定
        conf.setInt("top.n", 20);
        //2.main方法传参数
        // conf.setInt("top.n",Integer.parseInt(args[0]));
        job.setJarByClass(WordCount.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(wordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
       // job.setCombinerClass(wordReducer.class);
        job.setReducerClass(wordReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class wordMapper extends Mapper<Object, Text, Text, IntWritable> {
        public static List<String> filter = new ArrayList<String>();
        public static List<String> arraylist = new LinkedList<String>();
        public static List<String> arraylist2 = new LinkedList<>();

        public static int count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = null;
            FileSplit fs = (FileSplit) context.getInputSplit();
            filename = fs.getPath().getName();

            //String filefilterdata ="/Users/qishi/Desktop/Mapreducer/inputData/stopwords.txt";
            if (filename.equals("stopwords.txt")) {
                System.out.println("路径名" + fs.getPath().toString().split(":")[1]);
                BufferedReader br = new BufferedReader(new FileReader(fs.getPath().toString().split(":")[1]));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] strings = line.split("\t");
                    filter.add(strings[0]);
                }
                br.close();
            }
            // String file2data="/Users/qishi/Desktop/Mapreducer/inputData/task1-input2.txt";

        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filename = null;
            FileSplit fs = (FileSplit) context.getInputSplit();
            filename = fs.getPath().getName();
            String line = value.toString();
            String[] splits = null;
            if (filename.equals("task1-input1.txt")) {
                //(space)\t\n\r\f
                splits = line.split("[\\s\\xA0]+");
                for (String word : splits) {
                    if (!filter.contains(word)) {
                        arraylist.add(word);
                    }
                }

            } else if (filename.equals("task1-input2.txt")) {
                splits = line.split("[\\s\\xA0]+");
                for (String word : splits) {
                    if (!filter.contains(word)) {
                        arraylist2.add(word);
                    }
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<String> list = receiveCollectionList(arraylist, arraylist2);
            for (String s : arraylist) {
                System.out.println(s);
            }
            for (String s : list) {
                context.write(new Text(s), new IntWritable(1));
            }
        }

        public static List<String> receiveCollectionList(List<String> firstArrayList, List<String> secondArrayList) {
            List<String> resultList = new ArrayList<String>();
            LinkedList<String> result = new LinkedList<String>(firstArrayList);// 大集合用linkedlist
            HashSet<String> othHash = new HashSet<String>(secondArrayList);// 小集合用hashset
            Iterator<String> iter = result.iterator();// 采用Iterator迭代器进行数据的操作
            while (iter.hasNext()) {
                if (!othHash.contains(iter.next())) {
                    iter.remove();
                }
            }
            resultList = new ArrayList<String>(result);
            return resultList;
        }
    }
    public static class wordReducer  extends Reducer<Text, IntWritable,IntWritable,Text> {
        private TreeMap<Word,Object> tm = new TreeMap<Word,Object>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Word word=new Word();
            word.set(key.toString(),sum);
            tm.put(word,null);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int top = conf.getInt("top.n", 20);
            Set<Map.Entry<Word, Object>> entrySet = tm.entrySet();
            int i=0;
            for (Map.Entry<Word,Object> entry:entrySet) {
                context.write(new IntWritable(entry.getKey().getCount()),new Text(entry.getKey().getPage()));
                i++;
                //取到排名前n条
                if(i==top) return;
            }
        }
    }

    public class Word  implements Comparable<Word>{
        private String page;
        private int count;
    
        public void set(String page, int count) {
            this.page = page;
            this.count = count;
        }
    
        public String getPage() {
            return page;
        }
    
        public void setPage(String page) {
            this.page = page;
        }
    
        public int getCount() {
            return count;
        }
    
        public void setCount(int count) {
            this.count = count;
        }
    
        public int compareTo(Word o) {
            return o.getCount()-this.count==0
                    ?this.page.compareTo(o.getPage())
                    :o.getCount()-this.count;
        }
    }
}
