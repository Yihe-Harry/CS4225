package TopkCommonWords;

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
 * @author: 
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
        job.setJarByClass(TopkCommonWords.class);
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
                   // System.out.println("--word"+word);
                    if (!filter.contains(word)) {
                        if(word!=" "&&word.length()!=0) {
                            context.write(new Text(filename + "9" + word), new IntWritable(1));
                        }
                    }
                }

            } else if (filename.equals("task1-input2.txt")) {
                splits = line.split("[\\s\\xA0]+");
                for (String word2 : splits) {
                    if (!filter.contains(word2)) {
                        if(word2!=" " &&word2.length()!=0) {
                            context.write(new Text(filename + "9" + word2), new IntWritable(1));
                        }
                    }
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }


    }
    public static class wordReducer  extends Reducer<Text, IntWritable,IntWritable,Text> {
        private TreeMap<Word,Object> tm = new TreeMap<Word,Object>();
        public static Map<String,Integer> hashMap1 = new HashMap<>();
        public static Map<String,Integer> hashMap2 = new HashMap<>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int sum=0;
           String fileName = key.toString().split("9")[0];

            System.out.println(fileName);
            //System.out.println(word1);
            if(fileName.equals("task1-input1.txt")){
                for (IntWritable value:values){
                    sum=sum+value.get();
                }
                System.out.println(sum);
                String word1 = key.toString().split("9")[1];
               hashMap1.put(word1,sum);
            }
            if(fileName.equals("task1-input2.txt")){
                for (IntWritable value:values){
                    sum=sum+value.get();
                }
                String word2 = key.toString().split("9")[1];
                hashMap2.put(word2,sum);
            }
            List<Word> words = receiveCollectionList(hashMap1, hashMap2);
            // Word word = new Word();
            for(Word s:words){
                tm.put(s,null);
            }
        }
        public static List<Word> receiveCollectionList(Map<String,Integer> firstArrayList, Map<String,Integer> secondArrayList) {
            List<Word> resultList=new ArrayList<>();
            for (Map.Entry<String, Integer> entry : secondArrayList.entrySet()) {
                firstArrayList.merge(entry.getKey(),entry.getValue(),(oldValue, newValue) ->oldValue>newValue ? newValue :oldValue);
            }

            for (Map.Entry<String, Integer> entry : firstArrayList.entrySet()) {
                String k = entry.getKey();
                int v = entry.getValue();
               Word word = new Word(k, v);
                resultList.add(word);
            }
            return resultList;
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

}