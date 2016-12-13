/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import net.sf.j2s.ajax.SimplePipeRunnable;
import im.webuzz.pilet.HttpResponse;


/**
 * Pipe request for Simple Pipe.
 * 
 * Considered as internal class.
 * 
 * ATTENTION: Only suggested to use this feature for applications of server
 * management.
 * 
 * @author zhourenjian
 *
 */
class PipeRequest {

	public String key;
	public SimplePipeRunnable pipe;
	public int priority;
	public int items;
	public long beforeLoop;
	public long lastLiveDetected;
	public char type;
	public StringBuffer buffer;
	public String contentType;
	public String domain;
	public SimpleHttpRequest req;
	public HttpResponse resp;
	public boolean headerSent;
	public boolean closingSocket;
	public long sentSequence;
	public boolean chunking;
	
}
