import com.datashrimp.HelloMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class HelloMRTest {

    @Test
    public void test() throws Exception {
        // Windows运行MR，下载winutils hadoop已编译文件，并拷贝其中hadoop.dll到windows/system32目录下
        // 设置HADOOP_HOME变量（即winutils对应版本目录），或程序中设置一下变量
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.1" );
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("input");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        HelloMR helloMR = new HelloMR();
        helloMR.setConf(conf);

        int exitCode = helloMR.run(new String[] {
                input.toString(), output.toString()
        });
        Assert.assertEquals(exitCode, 0);
    }
}
