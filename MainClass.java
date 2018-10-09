import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

public class MainClass
{
    public static void main(String[] args) throws Exception {
        //Connecting to Redis server on localhost
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server sucessfully");
        //check whether server is running or not
        System.out.println("Server is running: "+jedis.ping());



        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();

        Dataset<Row> df = spark.read().format("csv").option("sep",",").option("header","true").load(args[0]);

        int count = 0;

        List<Row> rows = df.select("id","dateAdded","brand","colors").collectAsList();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        Pipeline pipeline = jedis.pipelined();
        for (Row row : rows)
        {
            System.out.println(++count);
            String key = "id:" + row.getString(0);
            pipeline.hset(key, "dateAdded", row.getString(1));
            pipeline.hset(key, "brand", row.get(2) != null ? row.getString(2) : "null");
            pipeline.hset(key, "colors", row.get(3) != null ? row.getString(3) : "null");


            String time = row.getString(1);
            long timeLong = simpleDateFormat.parse(time).getTime();

            pipeline.zadd("TimeSeries", timeLong, key);


            String colorList = row.getString(3);
            if(colorList != null)
            {
                String[] colorSplit = colorList.split(",");
                for (String color : colorSplit)
                {
                    pipeline.sadd("colors:" + color, key);
                }
            }
        }
        pipeline.sync();

        spark.close();
    }
}
