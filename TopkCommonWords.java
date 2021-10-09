import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static class wordMapper extends Mapper<Object, Text, Text, IntWritable> {

        public static List<String> filter = new ArrayList<String>();
        public static List<String> arraylist = new LinkedList<String>();
        public static List<String> arraylist2 = new LinkedList<>();

        public static int count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filefilterdata = "";
            BufferedReader br = new BufferedReader(new FileReader(filefilterdata));
            String line;
            while ((line = br.readLine()) != null) {
                String[] strings = line.split("\t");
                filter.add(strings[0]);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filename = null;
            FileSplit fs = (FileSplit) context.getInputSplit();
            filename = fs.getPath.toString();
            String [] splits = null;
            if (filename.equals("task-input1.txt")) {
                split = line.split("[\\s\\xA0]+");
                for (String word : splits) {
                    if (!filter.contains(words)) {
                        arraylist.add(word);
                    }
                }
            } else if (filename.equals("task-input2.txt")) {
                splits = line.split("[\\s\\xA0]+");
                for (String word : splits) {
                    if (!filter.contains(words)) {
                        arraylist2.add(word);
                    }
                }
            }
        }

        protected void cleanup (Context context) throws IOException, InterruptedException {
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
            LinkedList<String> result = new LinkedList<String>(firstArrayList);
            HashSet<String> othHash = new HashSet<String> (secondArrayList);
            Iterator<String> iter = result.iterator();
            while (iter.hasNext()) {
                if (!othHash.contains(iter.next())) {
                    iter.remove;
                }
            }
            resultList = new ArrayList<String> (result);
            return resultList;
        }
    }

    public static class wordReducer
            extends Reducer<Text, IntWritable,Text,IntWritable> {
        
        private TreeMap<Word,Object> tm = new TreeMap<Word,Object>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Word word = new Word();
            word.set(key.toString(), sum);
            tm.put(word, null);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration;
            int top = conf.getInt("top.n", 20);
            Set<Map.Entry<Word, Object>> entrySet = tm.entrySet();
            int i = 0;
            for (Map.Entry<Word, Object> entry : entrySet) {
                context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
                i++;
                if (i == top) {
                    return;
                }
            }
        }
    }

}
