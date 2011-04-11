package backtype.cascading.tap;

import backtype.hadoop.pail.BinaryPailStructure;
import backtype.hadoop.pail.DefaultPailStructure;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.PailFormatFactory;
import backtype.hadoop.pail.PailOutputFormat;
import backtype.hadoop.pail.PailPathLister;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;
import backtype.support.CascadingUtils;
import backtype.support.Utils;
import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.hadoop.TupleSerialization;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;


public class PailTap extends Hfs implements FlowListener {
    private static Logger LOG = Logger.getLogger(PailTap.class);

    public static PailSpec makeSpec(PailSpec given, PailStructure structure) {
        if(given==null) {
            return PailFormatFactory.getDefaultCopy().setStructure(structure);
        } else {
            return given.setStructure(structure);
        }
    }

    public static class PailTapOptions implements Serializable {
        public PailSpec spec = null;
        public String fieldName = "bytes";
        public List<String>[] attrs = null;
        public PailPathLister lister = null;

        public PailTapOptions() {
            
        }

        public PailTapOptions(PailSpec spec, String fieldName, List<String>[] attrs, PailPathLister lister) {
            this.spec = spec;
            this.fieldName = fieldName;
            this.attrs = attrs;
            this.lister = lister;
        }
    }


    public class PailScheme extends Scheme {
        private PailTapOptions _options;

        public PailScheme(PailTapOptions options) {
            super(new Fields("pail_root", options.fieldName), Fields.ALL);
            _options = options;
        }

        public PailSpec getSpec() {
            return _options.spec;
        }

        @Override
        public void sourceInit(Tap tap, JobConf conf) throws IOException {
            Pail p = new Pail(_pailRoot); //make sure it exists
            conf.setInputFormat(p.getFormat().getInputFormatClass());
            PailFormatFactory.setPailPathLister(conf, _options.lister);
        }

        @Override
        public void sinkInit(Tap tap, JobConf conf) throws IOException {
            conf.setOutputFormat(PailOutputFormat.class);
            Utils.setObject(conf, PailOutputFormat.SPEC_ARG, getSpec());
            Pail.create(getFileSystem(conf), _pailRoot, getSpec(), true);
        }

        @Override
        public Tuple source(Object k, Object v) {
            String relPath = ((Text) k).toString();
            Comparable value = deserialize((BytesWritable) v);
            return new Tuple(relPath, value);
        }

        private transient BytesWritable bw;
        private transient Text keyW;

        @Override
        public void sink(TupleEntry tuple, OutputCollector output) throws IOException {
            Comparable obj = tuple.get(0);
            String key;
            //a hack since byte[] isn't natively handled by hadoop
            if(getStructure() instanceof DefaultPailStructure) {
                key = getCategory(obj);
            } else {
                key = Utils.join(getStructure().getTarget(obj), "/") + getCategory(obj);
            }
            if(bw==null) bw = new BytesWritable();
            if(keyW==null) keyW = new Text();
            serialize(obj, bw);
            keyW.set(key);
            output.collect(keyW, bw);
        }


        protected Comparable deserialize(BytesWritable record) {
            PailStructure structure = getStructure();
            if(structure instanceof BinaryPailStructure) {
                return record;
            } else {
                return (Comparable) structure.deserialize(Utils.getBytes(record));
            }
        }

        protected void serialize(Comparable obj, BytesWritable ret) {
            if(obj instanceof BytesWritable) {
                ret.set((BytesWritable) obj);
            } else {
                byte[] b = getStructure().serialize(obj);
                ret.set(b, 0, b.length);
            }
        }

        private transient PailStructure _structure;

        public PailStructure getStructure() {
            if(_structure==null) {
                if(getSpec()==null) {
                    _structure = PailFormatFactory.getDefaultCopy().getStructure();
                } else {
                    _structure = getSpec().getStructure();
                }
            }
            return _structure;
        }

    }

    private String _pailRoot;
    private PailTapOptions _options;

    protected String getCategory(Comparable obj) {
        return "";
    }

    public PailTap(String root, PailTapOptions options) {
        _options = options;
        setStringPath(root);
        setScheme(new PailScheme(options));
        _pailRoot = root;
    }

    public PailTap(String root) {
        this(root, new PailTapOptions());
    }

    @Override
    public boolean deletePath(JobConf conf) throws IOException {
        throw new UnsupportedOperationException();
    }

    //no good way to override this, just had to copy/paste and modify
    @Override
    public void sourceInit(JobConf conf) throws IOException {
        Path root = getQualifiedPath(conf);
        if(_options.attrs!=null && _options.attrs.length>0) {
            Pail pail = new Pail(_pailRoot);
            for(List<String> attr: _options.attrs) {
                String rel = Utils.join(attr, "/");
                pail.getSubPail(rel); //ensure the path exists
                Path toAdd = new Path(root, rel);
                LOG.info("Adding input path " + toAdd.toString());
                FileInputFormat.addInputPath(conf, toAdd);
            }
        } else {
            FileInputFormat.addInputPath(conf, root);
        }

        getScheme().sourceInit(this, conf);
        makeLocal( conf, getQualifiedPath(conf), "forcing job to local mode, via source: " );
        TupleSerialization.setSerializations( conf );
    }

    private void makeLocal(JobConf conf, Path qualifiedPath, String infoMessage) {
      if( !conf.get( "mapred.job.tracker", "" ).equalsIgnoreCase( "local" ) && qualifiedPath.toUri().getScheme().equalsIgnoreCase( "file" ) )
      {
      if( LOG.isInfoEnabled() )
        LOG.info( infoMessage + toString() );

      conf.set( "mapred.job.tracker", "local" ); // force job to run locally
      }
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        if(_options.attrs!=null && _options.attrs.length>0) {
            throw new TapException("can't declare attributes in a sink");
        }
        super.sinkInit(conf);
    }

    public void onCompleted(Flow flow) {
        try {
            //only if it's a sink
            if(flow.getFlowStats().isSuccessful() && CascadingUtils.isSinkOf(this, flow)) {
                Pail p = Pail.create(_pailRoot, ((PailScheme)getScheme()).getSpec(), false);
                FileSystem fs = p.getFileSystem();
                Path tmpPath = new Path(_pailRoot, "_temporary");
                if(fs.exists(tmpPath)) {
                    LOG.info("Deleting _temporary directory left by Hadoop job: " + tmpPath.toString());
                    fs.delete(tmpPath, true);
                }

                Path tmp2Path = new Path(_pailRoot, "_temporary2");
                if(fs.exists(tmp2Path)) {
                    LOG.info("Deleting _temporary2 directory: " + tmp2Path.toString());
                    fs.delete(tmp2Path, true);
                }

                Path logPath = new Path(_pailRoot, "_logs");
                if(fs.exists(logPath)) {
                    LOG.info("Deleting _logs directory left by Hadoop job: " + logPath.toString());
                    fs.delete(logPath, true);
                }
            }
        } catch(IOException e) {
            throw new TapException(e);
        }
    }

    public void onStarting(Flow flow) {}

    public void onStopping(Flow flow) {}

    public boolean onThrowable(Flow flow, Throwable thrwbl) {
        return false;
    }

    @Override
    public int hashCode() {
        return _pailRoot.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if(!getClass().equals(object.getClass())) {
            return false;
        }
        PailTap other = (PailTap) object;
        Set<List<String>> myattrs = new HashSet<List<String>>();
        if(_options.attrs!=null) {
            for(List<String> a: _options.attrs) {
                myattrs.add(a);
            }
        }
        Set<List<String>> otherattrs = new HashSet<List<String>>();
        if(other._options.attrs!=null) {
            for(List<String> a: other._options.attrs) {
                otherattrs.add(a);
            }
        }
        return _pailRoot.equals(other._pailRoot) && myattrs.equals(otherattrs);
    }

    
}
