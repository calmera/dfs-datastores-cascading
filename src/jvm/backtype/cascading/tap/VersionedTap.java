package backtype.cascading.tap;

import backtype.cascading.scheme.KeyValueByteScheme;
import backtype.hadoop.datastores.VersionedStore;
import backtype.support.CascadingUtils;
import cascading.flow.FlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class VersionedTap extends Hfs {
    public static enum TapMode { SOURCE, SINK }

    // sink-specific
    public TapMode mode;
    public Long version = null;

    // source-specific
    private String newVersionPath;

    public VersionedTap(String dir, String keyField, String valField, TapMode mode) throws IOException {
        super(new KeyValueByteScheme(new Fields(keyField, valField)), dir);
        this.mode = mode;
    }

    public VersionedTap setVersion(long version) {
        this.version = version;
        return this;
    }

    public String getOutputDirectory() {
        return getPath().toString();
    }

    public VersionedStore getStore(JobConf conf) throws IOException {
        return new VersionedStore(FileSystem.get(conf), getOutputDirectory());
    }

    public String getSourcePath(JobConf conf) {
        VersionedStore store;
        try {
            store = getStore(conf);
            return (version != null) ? store.versionPath(version) : store.mostRecentVersionPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
        super.sourceConfInit(process, conf);
        FileInputFormat.setInputPaths(conf, getSourcePath(conf));
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        super.sinkConfInit(process, conf);
        try {
            if (newVersionPath == null)
                newVersionPath = getStore(conf).createVersion();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FileOutputFormat.setOutputPath(conf, new Path(newVersionPath));
    }

    @Override
    public boolean resourceExists(JobConf jc) throws IOException {
        return getStore(jc).mostRecentVersion() != null;
    }

    @Override
    public boolean createResource(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean deleteResource(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getIdentifier() {
        String outDir = getOutputDirectory();
        String versionString = (version == null) ? "LATEST" : version.toString();
        return "manhattan:" + ((mode == TapMode.SINK) ? outDir : outDir + ":" + versionString);
    }

    @Override
    public boolean commitResource(JobConf conf) throws IOException {
        VersionedStore store = new VersionedStore(FileSystem.get(conf), getOutputDirectory());

        if (newVersionPath != null) {
            store.succeedVersion(newVersionPath);
            CascadingUtils.markSuccessfulOutputDir(new Path(newVersionPath), conf);
            newVersionPath = null;
        }

        return true;
    }

    @Override
    public boolean rollbackResource(JobConf conf) throws IOException {
        if (newVersionPath != null) {
            getStore(conf).failVersion(newVersionPath);
            newVersionPath = null;
        }

        return true;
    }
}
