package tv.floe.cephenes.plugins.flume;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.*;
import com.cloudera.flume.handlers.syslog.SyslogTcpSource;
import com.cloudera.flume.handlers.syslog.SyslogWireExtractor;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.google.common.base.Preconditions;

/**
 * I'm just getting a feel for writing a custom Flume source with this class
 * 
 * its based on the code of "SyslogTcpSource.java"
 * 
 * @author jpatterson
 *
 */
public class DemoTCPSource extends EventSource.Base {
	  public final static int DEMO_TCP_PORT = 555;
	  ServerSocket sock = null;
	  int port = DEMO_TCP_PORT; 
	  DataInputStream is;
	  long rejects = 0;

	  public DemoTCPSource(int port) {
	    this.port = port;
	  }

	  public DemoTCPSource() {
	  }

	  @Override
	  public void close() throws IOException {
	    sock.close();
	    sock = null;
	  }

	  @Override
	  public Event next() throws IOException {
	    Event e = null;
	    while (true) {
	      try {
	        e = SyslogWireExtractor.extractEvent(is);
	        updateEventProcessingStats(e);
	        return e;
	      } catch (EventExtractException ex) {
	        rejects++;
	      }
	    }
	  }

	  @Override
	  public void open() throws IOException {
	    Preconditions.checkState(sock == null);
	    sock = new ServerSocket(port);
	    Socket client = sock.accept();
	    is = new DataInputStream(client.getInputStream());
	  }

	  public static SourceBuilder builder() {
	    return new SourceBuilder() {

	      @Override
	      public EventSource build(Context ctx, String... argv) {
	        int port = DEMO_TCP_PORT; // default udp port, need root permissions
	        // for this.
	        if (argv.length > 1) {
	          throw new IllegalArgumentException("usage: syslogTcp1([port no]) ");
	        }

	        if (argv.length == 1) {
	          port = Integer.parseInt(argv[0]);
	        }

	        return new SyslogTcpSource(port);
	      }

	    };
	  }
}