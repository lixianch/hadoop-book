import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by lixianch on 2016/8/11.
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(new Path(uri));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
            inputStream.seek(0);
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }

    }
}
