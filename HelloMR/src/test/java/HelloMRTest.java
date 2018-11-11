import com.datashrimp.HelloMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class HelloMRTest {

    @Test
    public void test() throws Exception {
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
