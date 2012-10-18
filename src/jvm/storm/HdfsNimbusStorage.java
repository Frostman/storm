package storm;

import backtype.storm.nimbus.INimbusStorage;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.base.Function;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.hadoop.thirdparty.guava.common.collect.Collections2.transform;

/**
 * @author slukjanov
 */
public class HdfsNimbusStorage implements INimbusStorage {
    private static final Logger LOG = Logger.getLogger(HdfsNimbusStorage.class);

    public static final String NIMBUS_STORAGE_HDFS_PATH = "nimbus.storage.hdfs.path";
    public static final String NIMBUS_STORAGE_HDFS_USER = "nimbus.storage.hdfs.user";

    private FileSystem fs;

    @Override
    public void init(Map conf) {
        try {
            String pathString = (String) conf.get(NIMBUS_STORAGE_HDFS_PATH);
            LOG.info("Using HDFS Nimbus storage: '" + pathString + "'");
            String user = (String) conf.get(NIMBUS_STORAGE_HDFS_USER);
            if (!Strings.isNullOrEmpty(user)) {
                System.setProperty("HADOOP_USER_NAME", user);
            }
            fs = new Path(pathString).getFileSystem(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream open(String path) {
        try {
            return fs.open(new Path(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputStream create(String path) {
        try {
            return fs.create(new Path(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> list(String path) {
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(path));
            if (fileStatuses == null) {
                return Collections.emptyList();
            }

            return new ArrayList<String>(transform(asList(fileStatuses), new Function<FileStatus, String>() {
                @Override
                public String apply(FileStatus fileStatus) {
                    return fileStatus.getPath().getName();
                }
            }));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mkdirs(String path) {
        try {
            fs.mkdirs(new Path(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isSupportDistributed() {
        return true;
    }
}
