package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.apache.commons.httpclient.URI.space;

/**
 * @description:
 * @author: 龙辉辉
 * @create: 2021-09-26 22:13
 */
public class wordMapper extends Mapper<Object, Text, Text, IntWritable> {
    public  static List<String> filter=new ArrayList<String>();
    public static List<String> arraylist=new LinkedList<String>();
    public static List<String> arraylist2=new LinkedList<>();

    public static int count=0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String filefilterdata ="/Users/qishi/Desktop/Mapreducer/inputData/stopwords.txt";
       // String file2data="/Users/qishi/Desktop/Mapreducer/inputData/task1-input2.txt";
        BufferedReader br = new BufferedReader(new FileReader(filefilterdata));
        String line;
        while ((line=br.readLine())!=null) {
            String[] strings =line.split("\t");
            filter.add(strings[0]);
        }
        br.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String filename = null;
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
        String line = value.toString();
        String [] splits = null;
        if(filename.equals("task1-input1.txt")){
            //(space)\t\n\r\f
          splits=line.split("[\\s\\xA0]+");
            for(String word:splits){
                if(!filter.contains(word)) {
                    arraylist.add(word);
                }
            }

        }else if(filename.equals("task1-input2.txt")) {
            splits = line.split( "[\\s\\xA0]+");
            for (String word : splits) {
                if(!filter.contains(word)) {
                    arraylist2.add(word);
                }
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<String> list = receiveCollectionList(arraylist, arraylist2);
        for(String s: arraylist){
            System.out.println(s);
        }
        for (String  s:list){
           context.write(new Text(s),new IntWritable(1));
        }
    }
    public static List<String> receiveCollectionList(List<String> firstArrayList, List<String> secondArrayList) {
        List<String> resultList = new ArrayList<String>();
        LinkedList<String> result = new LinkedList<String>(firstArrayList);// 大集合用linkedlist
        HashSet<String> othHash = new HashSet<String>(secondArrayList);// 小集合用hashset
        Iterator<String> iter = result.iterator();// 采用Iterator迭代器进行数据的操作
        while(iter.hasNext()) {
            if(!othHash.contains(iter.next())) {
                iter.remove();
            }
        }
        resultList = new ArrayList<String>(result);
        return resultList;
    }
}
