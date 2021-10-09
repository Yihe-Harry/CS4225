package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @description:
 * @author: 龙辉辉
 * @create: 2021-09-26 23:05
 */
public class wordReducer  extends Reducer<Text, IntWritable,Text,IntWritable> {
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
            context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
            i++;
            //取到排名前n条
            if(i==top) return;
        }
    }
}
