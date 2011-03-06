package tv.floe.lumberyard.plugins.flume;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FlushingSequenceFileWriter;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.hdfs.WriteableEventKey;
import com.cloudera.util.FileUtil;
import com.google.common.base.Preconditions;



/**
 * Experimental Flume to Lumberyard sink
 * 
 * - does not guarantee all data will show up
 * - data has a window to show up in the sink
 * - there is a "pool" or window per sensor
 * - once the window has expired, we flush data in the current window as a write to Lumberyard
 * - we then slide the window forward N segments
 * 
 * Current state:
 * 
 * - very broken and just a sketch; read: do not use
 * 
 * @author jpatterson
 *
 */
public class LumberyardSink extends EventSink.Base {
	
	  static final Logger LOG = LoggerFactory.getLogger(SeqfileEventSink.class);

	  private SequenceFile.Writer writer;
	  private long count = 0;
	  private boolean bufferedIO = false;
	  private String tag;
	  private File f;
	  
	  /*
	   * TODO
	   * 
	   * setup queues/windows/lanes of data
	   * 
	   * for now we'll write the data to a sequence file in a block to make testing easier
	   * 
	   */

	  /*
	   * This method is to debug write to a seqeunce file
	   */
	  public LumberyardSink(File f) throws IOException {
	    this.f = f;
	    this.tag = f.getName();
	    LOG.info("constructed new seqfile event sink: file=" + f);
	  }
	  
	  /*
	   * real ctor
	   */
	  public LumberyardSink(String HBaseURI, String LumberyardTablename ) {
		  
		  
		  
	  }
	  

	  /*
	   * This is assumed to be open
	   */
	  public void open() throws IOException {
	    LOG.debug("opening " + f);

	    // need absolute file to get full path. (pwd files have not parent file)
	    // make directories if necessary
	    if (!FileUtil.makeDirs(f.getAbsoluteFile().getParentFile())) {
	      throw new IOException("Unable to create directory"
	          + f.getAbsoluteFile().getParentFile() + " for writing");
	    }

	    Configuration conf = FlumeConfiguration.get();
	    try {
	      writer = FlushingSequenceFileWriter.createWriter(conf, f,
	          WriteableEventKey.class, WriteableEvent.class);
	    } catch (FileNotFoundException fnfe) {
	      LOG.error("Possible permissions problem when creating " + f, fnfe);
	      throw fnfe;
	    }
	  }

	  /**
	   * @throws IOException
	   * 
	   */
	  public void close() throws IOException {
	    LOG.debug("closing " + f);
	    if (writer == null) {
	      // allow closing twice.
	      return;
	    }
	    writer.close();
	    writer = null;
	    LOG.info("closed " + f);
	  }

	  public void append(Event e) throws IOException, InterruptedException  {
	    Preconditions.checkNotNull(writer,
	        "Attempt to append to a sink that is closed!");

	    WriteableEvent we = new WriteableEvent(e);
	    writer.append(we.getEventKey(), we);

	    // flush if we are not buffering
	    if (!bufferedIO)
	      writer.sync(); // this isn't flushing or sync'ing on local file system :(

	    count++;
	    super.append(e);
	  }

	  public void setBufferIO(boolean bufferedIO) {
	    this.bufferedIO = bufferedIO;
	  }

	  public String getTag() {
	    return tag;
	  }

	  public static SinkBuilder builder() {
	    return new SinkBuilder() {
	      @Override
	      public EventSink build(Context context, String... args) {
	        Preconditions.checkArgument(args.length == 1,
	            "usage: seqfile(filename)");
	        try {
	          return new SeqfileEventSink(new File(args[0]));
	        } catch (IOException e) {
	          throw new IllegalArgumentException("Unable to open file for writing "
	              + args[0]);
	        }
	      }
	    };
	  }
	

}
