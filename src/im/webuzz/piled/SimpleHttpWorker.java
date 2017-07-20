package im.webuzz.piled;

import im.webuzz.pilet.HttpConfig;
import im.webuzz.pilet.HttpLoggingUtils;
import im.webuzz.pilet.HttpRequest;
import im.webuzz.pilet.HttpResponse;
import im.webuzz.pilet.HttpWorkerUtils;
import im.webuzz.pilet.IRequestMonitor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

import net.sf.j2s.ajax.CompoundPipeRunnable;
import net.sf.j2s.ajax.CompoundPipeSession;
import net.sf.j2s.ajax.ISimpleCacheable;
import net.sf.j2s.ajax.ISimpleCometable;
import net.sf.j2s.ajax.ISimplePipePriority;
import net.sf.j2s.ajax.ISimpleRequestBinding;
import net.sf.j2s.ajax.ISimpleRequestInfoBinding;
import net.sf.j2s.ajax.SimpleFilter;
import net.sf.j2s.ajax.SimplePipeHelper;
import net.sf.j2s.ajax.SimplePipeRequest;
import net.sf.j2s.ajax.SimplePipeRunnable;
import net.sf.j2s.ajax.SimplePipeSequence;
import net.sf.j2s.ajax.SimpleRPCRunnable;
import net.sf.j2s.ajax.SimpleRPCUtils;
import net.sf.j2s.ajax.SimpleSerializable;

public class SimpleHttpWorker extends HttpWorker {

	private static Map<String, Long> lostPipes = new ConcurrentHashMap<String, Long>();

	SimplePipeMonitor monitor;

	private static SimpleFilter NO_DELTA_FILTER = new SimpleFilter() {
		
		public boolean accept(String field) {
			return true;
		}

		public boolean ignoreDefaultFields() {
			return false;
		}
	
	};
	
	public static void initialize() {
		try {
			// SimpleNIORequest.initialize();
			// Use reflection to avoid dependency on NIO HTTP Request implementation
			Class<?> nioReqClass = Class.forName("im.webuzz.nio.SimpleNIORequest");
			if (nioReqClass != null) {
				Method initMethod = nioReqClass.getMethod("initialize");
				if (initMethod != null) {
					initMethod.invoke(nioReqClass);
				}
			}
		} catch (Throwable e) {
		}
	}
	
	@Override
	protected HttpRequest makeHttpRequest() {
		return new SimpleHttpRequest();
	}

	/*
	 * Check expired requests and close it gracefully. Invoked every 10s
	 */
	protected void checkExpiredRequests() {
		boolean pipeSupported = PipeConfig.simplePipeSupported;
		boolean monitorSupported = PiledConfig.remoteMonitorSupported;
		long now = System.currentTimeMillis();
		Set<SimpleHttpRequest> toRemoveds = new HashSet<SimpleHttpRequest>();
		for (Iterator<?> iterator = requests.values().iterator(); iterator.hasNext();) {
			SimpleHttpRequest req = (SimpleHttpRequest) iterator.next();
			if (req.done) {
				if (/*(req.keepAliveMax <= 1 || req.requestCount >= req.keepAliveMax) // HTTP connection closed
						&& */now - Math.max(req.created, req.lastSent) > req.keepAliveTimeout * 1000) { // timeout
					toRemoveds.add(req);
				} else if ((req.keepAliveMax <= 1 || req.requestCount >= req.keepAliveMax) // HTTP connection closed
						&& req.created < req.lastSent && now - req.lastSent > 5000) { // data sent
					toRemoveds.add(req);
				}
			} else { // not finished response yet
				if (!req.comet && now - Math.max(req.created, req.lastSent) > 2 * HttpConfig.aliveTimeout * 1000) { // normally timeout
					toRemoveds.add(req);
				} else if (pipeSupported && req.comet && req.pipeKey != null) {
					if (PipeConfig.pipeSwitching && req.pipeSwitching) {
						lostPipes.remove(req.pipeKey);
					}
					if (now - req.pipeLastNotified > PipeConfig.queryTimeout) {
						SimplePipeHelper.notifyPipeStatus(req.pipeKey, true);
						req.pipeLastNotified = now;
					}
				}
			}
		}
		for (Iterator<?> iterator = closeSockets.values().iterator(); iterator.hasNext();) {
			SimpleHttpRequest req = (SimpleHttpRequest) iterator.next();
			if (req.created < req.lastSent && now - req.created > req.keepAliveTimeout * 1000) {
				toRemoveds.add(req);
			} else if (now - req.created > 2 * req.keepAliveTimeout * 1000) {
				toRemoveds.add(req);
			}
		}

		final List<IRequestMonitor> monitors = monitorSupported ? new ArrayList<IRequestMonitor>() : null;
		final Set<String> pipeKeys = pipeSupported ? new HashSet<String>() : null;
		if (pipeSupported && PipeConfig.pipeSwitching) {
			// Check whether pipes in lostPipes are expired, if so, we will call #pipeLost later
			for (Iterator<String> itr = lostPipes.keySet().iterator(); itr.hasNext();) {
				String key = (String) itr.next();
				Long time = lostPipes.get(key);
				if (time != null && now - time.longValue() > PipeConfig.pipeSwitchingMaxInterval) {
					itr.remove(); //lostPipes.remove(key);
					pipeKeys.add(key);
				}
			}
		}
		
		for (SimpleHttpRequest req : toRemoveds) {
			/*HttpRequest r = */requests.remove(req.socket);
			/*
			if (r != null && !r.done) {
				HttpWorkerUtils.send408RequestTimeout(req, dataEvent, closeSockets);
				HttpLoggingUtils.addLogging(req.host, req, 408, 0);
			}
			*/
			/*r = */closeSockets.remove(req.socket);
			/*
			if (r == null) {
				closeSockets.remove(null);
			}
			*/
			if (monitorSupported && !req.done && req.monitor != null) {
				monitors.add(req.monitor);
			}
			if (pipeSupported && req.comet && req.pipeKey != null
					&& SimplePipeRequest.PIPE_TYPE_CONTINUUM == req.pipeType) {
				// if supports pipe switching, wait until pipe switching interval gap
				if (PipeConfig.pipeSwitching && req.pipeSwitching) {
					lostPipes.put(req.pipeKey, Long.valueOf(now));
				} else {
					pipeKeys.add(req.pipeKey);
				}
			}
			// notify Piled server to close specific socket
			server.send(req.socket, ZERO_BYTES);
		}
		if ((monitorSupported && monitors.size() > 0) || (pipeSupported && pipeKeys.size() > 0)) {
			try {
				workerPool.execute(new Runnable() {
					@Override
					public void run() {
						if (PipeConfig.simplePipeSupported && pipeKeys != null) {
							for (String pipeKey : pipeKeys) {
								SimplePipeRunnable pipe = SimplePipeHelper.getPipe(pipeKey);
								if (pipe != null) {
									pipe.pipeLost();
									SimplePipeHelper.removePipe(pipeKey);
								}
							}
						}
						if (PiledConfig.remoteMonitorSupported && monitors != null) {
							for (IRequestMonitor monitor : monitors) {
								monitor.requestClosedByRemote();
							}
						}
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
				// UNSAFE: worker may be frozen, unless we comment out heavy tasks
				if (pipeSupported && pipeKeys.size() > 0) {
					for (String pipeKey : pipeKeys) {
						SimplePipeRunnable pipe = SimplePipeHelper.getPipe(pipeKey);
						if (pipe != null) {
							// pipe.pipeLost(); // might be heavy task
							SimplePipeHelper.removePipe(pipeKey);
						}
					}
				}
				/*
				if (monitorSupported && monitors.size() > 0) {
					for (IRequestMonitor monitor : monitors) {
						// monitor.requestClosedByRemote(); // might be heavy task
					}
				}
				// */
			}
		}
	}
	
	/*
	 * Update on response sent or response socket has been closed.
	 */
	protected void updateRequestResponse(ServerDataEvent dataEvent) {
		long now = System.currentTimeMillis();
		if (dataEvent.count != 0) {
			// notify request that bytes have been sent
			HttpRequest req = requests.get(dataEvent.socket);
			if (req == null) {
				req = closeSockets.get(dataEvent.socket);
			}
			if (req == null) {
				return;
			}
			req.lastSent = now;
			if (dataEvent.count > 0) {
				req.sending -= dataEvent.count;
				return;
			} else {
				req.sending -= -dataEvent.count;
				if (!(req.keepAliveMax == 0 && req.keepAliveTimeout == 0)) { // not "Connection: close"
					return;
				} else if (!req.done) { // HTTP 1.0
					// Server big file will be split into several packets for sending. If one packet
					// is sent, it will be notified that this packet is sent completed, but there are
					// chances that other packets are not being put into queue yet. Wait until req.done
					// is true before notifying that HTTP 1.0 request to close.
					return;
				} else if (req.sending <= 0) { // only after given HTTP 1.0 response is finished.
					// All data is sent and can be closed, tell server to close socket
					server.send(req.socket, ZERO_BYTES);
					// Continue rest lines to close this connection
				}
			}
		}
		
		// else dataEvent.count == 0
		// specific socket requests are being closed and should be removed.
		
		// there are no needs to close socket physically, Piled server is just
		// notifying worker that this socket is being closed physically on its
		// side.
		
		String pipeKey = null;
		SimpleHttpRequest req = (SimpleHttpRequest) requests.remove(dataEvent.socket);
		boolean removedFromCloseSockets = false;
		if (req == null) {
			req = (SimpleHttpRequest) closeSockets.remove(dataEvent.socket);
			removedFromCloseSockets = true;
		}
		if (req != null) {
			req.closed = now;
			if (PipeConfig.simplePipeSupported && req.comet && req.pipeKey != null
					&& SimplePipeRequest.PIPE_TYPE_CONTINUUM == req.pipeType) {
				if (PipeConfig.pipeSwitching && req.pipeSwitching) {
					// #checkExpiredRequests will call #pipeLost later
					lostPipes.put(req.pipeKey, Long.valueOf(now));
				} else {
					pipeKey = req.pipeKey;
				}
			}
			if (!removedFromCloseSockets) {
				closeSockets.remove(dataEvent.socket);
			}
		}
		final IRequestMonitor monitor = (PiledConfig.remoteMonitorSupported
				&& req != null && !req.done && req.monitor != null) ? req.monitor : null;
		if (monitor != null || pipeKey != null) {
			final String key = pipeKey; 
			try {
				workerPool.execute(new Runnable() {
					@Override
					public void run() {
						if (monitor != null) {
							monitor.requestClosedByRemote(); // might be a heavy task
						}
						if (key != null) {
							SimplePipeRunnable pipe = SimplePipeHelper.getPipe(key);
							if (pipe != null) {
								pipe.pipeLost(); // might be a heavy task
								SimplePipeHelper.removePipe(key);
							}
						}
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
				// UNSAFE: worker may be frozen, unless we comment out heavy tasks
				if (monitor != null) {
					// monitor.requestClosedByRemote(); // might be a heavy task
				}
				if (key != null) {
					SimplePipeRunnable pipe = SimplePipeHelper.getPipe(key);
					if (pipe != null) {
						// pipe.pipeLost(); // might be a heavy task
						SimplePipeHelper.removePipe(key);
					}
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	private byte[] getRequestBytes(final HttpRequest req) {
		byte[] bytes = null;
		if (req.requestData instanceof byte[]) {
			bytes = (byte[])req.requestData;
		} else { // if (req.requestData instanceof String) {
			String ss = (String) req.requestData;
			if (!"POST".equals(req.method)) {
				try {
					ss = URLDecoder.decode(ss);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Invalid post data: " + ss);
				}
			}
			bytes = ss.getBytes(HttpWorkerUtils.ISO_8859_1);
		}
		return bytes;
	}

	@SuppressWarnings("deprecation")
	private String getRequestString(final HttpRequest req) {
		String ss = null;
		if (req.requestData instanceof byte[]) {
			ss = new String((byte[])req.requestData, HttpWorkerUtils.ISO_8859_1);
		} else { // if (req.requestData instanceof String) {
			ss = (String) req.requestData;
		}
		if (!"POST".equals(req.method)) {
			try {
				ss = URLDecoder.decode(ss);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Invalid post data: " + ss);
			}
		}
		return ss;
	}

	protected void respondRequestWithData(HttpRequest req, HttpResponse rsp) {
		if (PipeConfig.simpleRPCSupported && req.url.endsWith("/r")) { // Optimize for Simple RPC
			req.cookies = null;
			service((SimpleHttpRequest) req, rsp, getRequestBytes(req), false);
		} else if (PipeConfig.simplePipeSupported && req.url.endsWith("/p")) { // Optimize for Simple Pipe
			req.cookies = null;
			pipe((SimpleHttpRequest) req, rsp, getRequestString(req));
		} else if (PipeConfig.simpleRPCSupported && (req.url.endsWith("piperpc")
				|| req.url.endsWith("/c") || req.url.endsWith("simplerpc"))) {
			req.cookies = null;
			service((SimpleHttpRequest) req, rsp, getRequestBytes(req), false);
		} else if (PipeConfig.simplePipeSupported && req.url.endsWith("simplepipe")) {
			req.cookies = null;
			pipe((SimpleHttpRequest) req, rsp, getRequestString(req));
		} else if (PipeConfig.simpleRPCSupported
				&& (req.url.endsWith("/j") || req.url.endsWith("simplejson"))) {
			req.cookies = null;
			service((SimpleHttpRequest) req, rsp, getRequestBytes(req), true);
		} else { // normal requests with query
			piletResponse(req, rsp);
		}
		chainingRequest(req, rsp);
	}

	private void pipe(final SimpleHttpRequest req, final HttpResponse resp, String ss) {
		int length = ss.length();
		if (length < 2) {
			HttpWorkerUtils.send400Response(req, resp);
			HttpLoggingUtils.addLogging(req.host, req, 410, length);
			return;
		}
		
		int keyStarted = -1;
		int keyStopped = -1;
		
		int typeStarted = -1;
		int typeStopped = -1;
		
		int domainStarted = -1;
		int domainStopped = -1;
		
		int hashStarted = -1;
		int hashStopped = -1;
		
		int seqStarted = -1;
		int seqStopped = -1;
		
		int unknownStarted = -1;
		int unknownStopped = -1;
		
		char kC = SimplePipeRequest.FORM_PIPE_KEY;
		char tC = SimplePipeRequest.FORM_PIPE_TYPE;
		char dC = SimplePipeRequest.FORM_PIPE_DOMAIN;
		char rC = SimplePipeRequest.FORM_PIPE_RANDOM;
		char sC = SimplePipeRequest.FORM_PIPE_SEQUENCE;
		char c1 = ss.charAt(0);
		for (int i = 1; i < length; i++) {
			char c0 = ss.charAt(i);
			if (keyStarted != -1 && keyStopped == -1) {
				if (c0 == '&') { // gotcha
					keyStopped = i;
				}
				continue;
			}
			if (typeStarted != -1 && typeStopped == -1) {
				if (c0 == '&') { // gotcha
					typeStopped = i;
				}
				continue;
			}
			if (domainStarted != -1 && domainStopped == -1) {
				if (c0 == '&') { // gotcha
					domainStopped = i;
				}
				continue;
			}
			if (hashStarted != -1 && hashStopped == -1) {
				if (c0 == '&') { // gotcha
					hashStopped = i;
				}
				continue;
			}
			if (seqStarted != -1 && seqStopped == -1) {
				if (c0 == '&') { // gotcha
					seqStopped = i;
				}
				continue;
			}
			if (unknownStarted != -1 && unknownStopped == -1) {
				if (c0 == '&') { // gotcha
					unknownStopped = i;
					unknownStarted = -1;
				}
				continue;
			}
			if (c0 == '=') { // ?=
				if (c1 == kC) {
					keyStarted = i + 1;
					keyStopped = -1;
				} else if (c1 == tC) {
					typeStarted = i + 1;
					typeStopped = -1;
				} else if (c1 == dC) {
					domainStarted = i + 1;
					domainStopped = -1;
				} else if (c1 == rC) {
					hashStarted = i + 1;
					hashStopped = -1;
				} else if (c1 == sC) {
					seqStarted = i + 1;
					seqStopped = -1;
				} else {
					unknownStarted = i + 1;
					unknownStopped = -1;
				}
				c0 = 0;
			}
			c1 = c0;
		}
		
		String key = null;
		if (keyStarted != -1) {
			if (keyStopped == -1) {
				keyStopped = length;
			}
			key = ss.substring(keyStarted, keyStopped);
		}
		if (key == null) {
			HttpWorkerUtils.send400Response(req, resp);
			HttpLoggingUtils.addLogging(req.host, req, 420, 0);
			return;
		}
		
		char type = SimplePipeRequest.PIPE_TYPE_CONTINUUM;
		if (typeStarted != -1) {
			if (typeStopped == -1) {
				typeStopped = length;
			}
			if (typeStopped > typeStarted) {
				type = ss.charAt(typeStarted);
			}
		}		

		String seq = null;
		if (seqStarted != -1) {
			if (seqStopped == -1) {
				seqStopped = length;
			}
			seq = ss.substring(seqStarted, seqStopped);
		}

		req.pipeKey = key;
		req.pipeType = type;
		req.pipeLastNotified = System.currentTimeMillis();
		
		if (seq != null) {
			try {
				req.pipeSequence = Long.parseLong(seq);
			} catch (NumberFormatException e) {
				//e.printStackTrace();
			}
		}
		
		if (SimplePipeRequest.PIPE_TYPE_NOTIFY == type) {
			/*
			 * Client send in "notify" request to execute #notifyPipeStatus, see below comments
			 */
			boolean updated = SimplePipeHelper.notifyPipeStatus(key, true); // update it!
			StringBuilder builder = new StringBuilder(32);
			builder.append("$p1p3b$ (\""); // $p1p3b$ = net.sf.j2s.ajax.SimplePipeRequest.pipeNotifyCallBack
			builder.append(key);
			builder.append("\", \"");
			builder.append(updated ? SimplePipeRequest.PIPE_STATUS_OK : SimplePipeRequest.PIPE_STATUS_LOST);
			builder.append("\", ");
			builder.append(req.pipeSequence);
			builder.append(");");
			//System.out.println("Notifying: " + System.currentTimeMillis() + " " + builder.toString());
			HttpWorkerUtils.pipeOut(req, resp, "text/javascript", null, builder.toString(), false);
			
			if (!updated || req.pipeSequence <= 0) { // not supporting pipe sequence
				return;
			}
			SimplePipeRunnable pipe = SimplePipeHelper.getPipe(key);
			if (pipe == null) { // should never run into this branch, as updated = true
				return;
			}
			List<SimpleSerializable> list = pipe.getPipeData();
			if (list == null) { // should never run into this branch
				return;
			}
			
			SimpleSerializable[] okEvts = null;
			int evtIdx = 0;
			synchronized (list) {
				int firstSeqIndex = pipe.getFirstPipeSequenceIndex();
				int size = list.size();
				if (0 < firstSeqIndex && firstSeqIndex < size) { // size > firstSeqIndex > 0
					long pipeSeq = Math.max(pipe.getSequence(), req.pipeSequence);
					int receivedIndex = findSequence(list, pipeSeq, firstSeqIndex);
					if (receivedIndex > 0) {
						boolean monitoring = pipe.isMonitoringEvents();
						if (monitoring) {
							okEvts = new SimpleSerializable[receivedIndex + 1];
						}
						for (int i = receivedIndex; i >= 0; i--) {
							SimpleSerializable e = list.remove(0);
							if (monitoring && !(e instanceof SimplePipeSequence)) {
								okEvts[evtIdx] = e;
								evtIdx++;
							}
						}
						// update indexes
						pipe.setFirstPipeSequenceIndex(0);
						pipe.setLastBufferedIndex(pipe.getLastBufferedIndex() - receivedIndex - 1);
					}
				} // else no needs to clear pipe data or update anything
			} // end of synchronized block
			
			markPipeDataOK(pipe, okEvts, evtIdx);
			
			if (req.pipeSequence > pipe.getSequence()) {
				pipe.setSequence(req.pipeSequence);
			}
			return;
		}
		
		String domain = null;
		if (domainStarted != -1) {
			if (domainStopped == -1) {
				domainStopped = length;
			}
			domain = ss.substring(domainStarted, domainStopped);
		}
		if (SimplePipeRequest.PIPE_TYPE_SUBDOMAIN_QUERY == type) { // subdomain query
			StringBuilder builder = new StringBuilder(1024);
			builder.append("<html><head><title></title></head><body>\r\n");
			builder.append("<script type=\"text/javascript\">");
			builder.append("p = new Object ();\r\n");
			builder.append("p.key = \"");
			builder.append(key);
			builder.append("\";\r\n");
			builder.append("p.originalDomain = document.domain;\r\n");
			builder.append("document.domain = \"");
			builder.append(domain);
			builder.append("\";\r\n");
			builder.append("var securityErrors = 0\r\n");
			builder.append("var lazyOnload = function () {\r\n");
			builder.append("try {\r\n");
			builder.append("var spr = window.parent.net.sf.j2s.ajax.SimplePipeRequest;\r\n");
			builder.append("eval (\"(\" + spr.subdomainInit + \") (p);\");\r\n");
			builder.append("eval (\"((\" + spr.subdomainLoopQuery + \") (p)) ();\");\r\n");
			builder.append("} catch (e) {\r\n");
			builder.append("securityErrors++;\r\n");
			builder.append("if (securityErrors < 100) {\r\n"); // 10s
			builder.append("window.setTimeout (lazyOnload, 100);\r\n");
			builder.append("};\r\n"); // end of if
			builder.append("};\r\n"); // end of catch
			builder.append("};\r\n"); // end of function
			builder.append("window.onload = lazyOnload;\r\n");
			builder.append("</script>\r\n");
			builder.append("</body></html>\r\n");
			HttpWorkerUtils.pipeOut(req, resp, "text/html", null, builder.toString(), false);
			return;
		}

		long beforeLoop = System.currentTimeMillis();
		
		req.comet = true;

		boolean supportChunking = PipeConfig.pipeChunking && req.v11;
		
		String contentType;

		PipeRequest r = new PipeRequest();
		r.headerSent = false;
		r.closingSocket = false;
		
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { // iframe
			contentType = "text/html";
			StringBuilder builder = new StringBuilder(256);
			builder.append("<html><head><title></title></head><body>\r\n");
			builder.append("<script type=\"text/javascript\">");
			if (domain != null) {
				builder.append("document.domain = \"");
				builder.append(domain);
				builder.append("\";\r\n");
			} else {
				builder.append("document.domain = document.domain;\r\n");
			}
			builder.append("function $ (s) { if (window.parent) window.parent.net.sf.j2s.ajax.SimplePipeRequest.parseReceived (s); }");
			builder.append("if (window.parent) eval (\"(\" + window.parent.net.sf.j2s.ajax.SimplePipeRequest.checkIFrameSrc + \") ();\");\r\n");
			builder.append("</script>\r\n");
			// flush
			r.closingSocket = SimplePipeUtils.pipeChunkedDataWithHeader(supportChunking, req, resp, contentType, false, builder.toString());
			r.headerSent = true;
		} else {
			if (SimplePipeRequest.PIPE_TYPE_QUERY == type
					|| SimplePipeRequest.PIPE_TYPE_CONTINUUM == type) {
				//contentType = "application/octet-stream";
				// iOS NSURLConnection buffers at least 512 bytes for application/octet-stream
				// https://stackoverflow.com/questions/9296221/what-are-alternatives-to-nsurlconnection-for-chunked-transfer-encoding
				/*
				 * NSURLConnection will work with chunked encoding, but has non-disclosed internal
				 * behaviour such that it will buffer first 512 bytes before it opens the connection
				 * and let anything through IF Content-Type in the response header is "text/html",
				 * or "application/octet-stream". This pertains to iOS7 at least.
				 * 
				 * Tested on iOS 9.2/June 2017, "application/octet-stream" is not working as expected
				 */
				contentType = "application/json";
			} else {
				contentType = "text/javascript";
			}
		}

		StringBuffer buffer = new StringBuffer(1024);
		
		int items = 0;
		int priority = 0;
        long lastLiveDetected = System.currentTimeMillis();

		int pipedStatus = 200;

		r.req = req;
		r.resp = resp;
		r.key = key;
		r.type = type;
		r.domain = domain;
		r.contentType = contentType;
		r.lastLiveDetected = lastLiveDetected;
		r.beforeLoop = beforeLoop;
		r.priority = priority;
		r.items = items;
		r.buffer = buffer;
		r.sentSequence = r.req.pipeSequence;
		r.chunking = supportChunking;

		r.pipe = SimplePipeHelper.getPipe(key);
		if (r.pipe != null && r.pipe.isPipeLive()) {
			// Keep pipe switching in request.
			req.pipeSwitching = r.pipe.supportsSwitching();
			if (PipeConfig.pipeSwitching && req.pipeSwitching && SimplePipeRequest.PIPE_TYPE_CONTINUUM == type
					&& System.currentTimeMillis() - Math.max(req.created, req.lastSent) > 2 * HttpConfig.aliveTimeout * 1000) {
				lostPipes.remove(key);
			}
			//r.pipe.updateStatus(true);
			r.pipe.keepPipeLive();
			r.pipe.pipeAlive = true;
			pipedStatus = doPipe(r, r.sentSequence > r.pipe.getSequence() ? false : true);
		} // else pipe is already closed or in other statuses
		/*
		if (SimplePipeHelper.notifyPipeStatus(key, true)) { // update it!
			pipedStatus = doPipe(r);
		} // else pipe is already closed or in other statuses
		// */
		if (pipedStatus == 200) {
			pipeEnd(buffer, key, type, beforeLoop, items);
			req.comet = false;
			HttpWorkerUtils.pipeOutBytes(req, resp, contentType, null,
					buffer.toString().getBytes(HttpWorkerUtils.ISO_8859_1), false);
		} else {
			long hash = -1;
			String hashStr = null;
			if (hashStarted != -1) {
				if (hashStopped == -1) {
					hashStopped = length;
				}
				hashStr = ss.substring(hashStarted, hashStopped);
			}
			if (hashStr != null) {
				try {
					hash = Long.parseLong(hashStr);
				} catch (NumberFormatException e) {
					System.out.println("Error request: " + ss);
					e.printStackTrace();
				}
			}
			SimplePipeRunnable p = SimplePipeHelper.checkPipeWithHash(key, hash);
			if (p == null) { // repeat attack?!
				req.comet = false;
				HttpWorkerUtils.send400Response(req, resp);
				//HttpLoggingUtils.addLogging(req.host, req, 430, 0);
			} else {
				boolean comet = SimplePipeRequest.PIPE_TYPE_SCRIPT == r.type
						|| SimplePipeRequest.PIPE_TYPE_CONTINUUM == r.type;
				if (comet && buffer.length() > 0) {
					// flush ...
					/*
					if (!r.headerSent) {
						r.closingSocket = HttpWorkerUtils.pipeChunkedHeader(r.req, r.de, r.contentType, false);
						r.headerSent = true;
					}
					// */
					String bufferedOutput = r.buffer.toString();
					int bufferedLength = bufferedOutput.length();
					if (bufferedLength > 0) { // There are chances that r.buffer is modified
						if (!r.headerSent) {
							r.closingSocket = SimplePipeUtils.pipeChunkedDataWithHeader(r.chunking, r.req, r.resp, r.contentType, false, bufferedOutput);
							r.headerSent = true;
						} else {
							SimplePipeUtils.pipeChunkedData(r.chunking, r.req, r.resp, bufferedOutput);
						}
						r.buffer.delete(0, bufferedLength);
					}
				} else if (comet && !r.headerSent) { // here buffer length = 0
					String bufferedOutput = SimplePipeUtils.output(type, key, SimplePipeRequest.PIPE_STATUS_OK);
					r.closingSocket = SimplePipeUtils.pipeChunkedDataWithHeader(r.chunking, r.req, r.resp, r.contentType, false, bufferedOutput);
					r.headerSent = true;
				}
				if (p instanceof IRequestMonitor) {
					req.monitor = (IRequestMonitor) p;
				}
				monitor.monitor(r);
			}
		}
	}

	/*
	 * Find the location of SimpleSequence with given sequence in given list. If last cached
	 * index of SimpleSequence object does not match, search list backward to find a correct
	 * location.
	 */
	private static int findSequence(List<SimpleSerializable> list, long targetSeq, int firstSeqIndex) {
		int receivedIndex = -1;
		int size = list.size();
		if (0 < firstSeqIndex && firstSeqIndex < size) {
			SimpleSerializable o = list.get(firstSeqIndex);
			if (o instanceof SimplePipeSequence) { // should always be true!
				long sequence = ((SimplePipeSequence) o).sequence;
				if (sequence == targetSeq) {
					// last marked sequence is less than current sequence, matched
					// try to clean data
					receivedIndex = firstSeqIndex;
				} else if (sequence > targetSeq) {
					// first sequence is already greater than given sequence
					receivedIndex = 0;
				} // else first sequence is less than given sequence, need searching
			}
		}
		if (receivedIndex == -1) {
			// Search list backward and find last sequence, and then try to send out
			// data from found sequence
			for (int i = size - 1; i >= 0; i--) {
				SimpleSerializable sso = list.get(i);
				if (sso instanceof SimplePipeSequence
						&& ((SimplePipeSequence) sso).sequence <= targetSeq) {
					receivedIndex = i;
					break;
				}
			}
		}
		return receivedIndex;
	}
	
	/**
	 * continuum, query, script, xss
	 * 
	 * type = continuum
	 * The connection will be kept open.
	 * 
	 * type = query
	 * The connection will not be kept. Each request stands for a query
	 * 
	 * type = script
	 * IFRAME object is used to simulate kept-open-connection
	 * 
	 * type = xss
	 * Cross Site Script pipe type, SimpleSerializable instances are
	 * wrapped in JavaScript string.
	 * 
	 * type = notify
	 * Notify that client (browser) still keeps the pipe connection.
	 */ 
	protected static int doPipe(PipeRequest r, boolean firstResponse) {
		SimplePipeRunnable pipe = SimplePipeHelper.getPipe(r.key);
        List<SimpleSerializable> list = pipe != null ? pipe.getPipeData() : null; //SimplePipeHelper.getPipeDataList(r.key);
        if (list == null) {
        	return 200;
        }

		StringBuilder builder = new StringBuilder(1024);
		int items = r.items;
		int priority = r.priority;
		long lastLiveDetected = r.lastLiveDetected;
		
		String key = r.key;
		char type = r.type;
		long beforeLoop = r.beforeLoop;
		
		long now = System.currentTimeMillis();
		
		boolean isScripting = SimplePipeRequest.PIPE_TYPE_SCRIPT == type;
		boolean isContinuum = SimplePipeRequest.PIPE_TYPE_CONTINUUM == type;
		pipe.setPipeMode(isContinuum ? SimplePipeRequest.MODE_PIPE_CONTINUUM : SimplePipeRequest.MODE_PIPE_QUERY);
		int maxItems = isScripting ? 5 * PipeConfig.maxItemsPerQuery : PipeConfig.maxItemsPerQuery;
		
		boolean live = SimplePipeHelper.isPipeLive(key);
		SimpleSerializable[] okEvts = null;
		int evtIdx = 0;
		synchronized (list) {
			boolean supportsSequence = r.sentSequence > 0;
			// begin compacting data in list, remove received data
			if (firstResponse && list.size() > 0 && supportsSequence) {
				// Only first response will trigger compacting, later compacting will be
				// triggered by /u/p?k=######&s=###&t=n&r=## request
				// If supports sequence, try to clean data prior to given sequence
				long pipeSeq = pipe.getSequence();
				int seqIndex = pipe.getFirstPipeSequenceIndex();
				int receivedIndex = findSequence(list, pipeSeq, seqIndex);
				if (receivedIndex > 0) {
					boolean monitoring = pipe.isMonitoringEvents();
					if (monitoring) {
						okEvts = new SimpleSerializable[receivedIndex + 1];
					}
					for (int i = receivedIndex; i >= 0; i--) {
						SimpleSerializable e = list.remove(0);
						if (monitoring && !(e instanceof SimplePipeSequence)) {
							okEvts[evtIdx] = e;
							evtIdx++;
						}
					}
					// update indexes
					pipe.setFirstPipeSequenceIndex(0);
					pipe.setLastBufferedIndex(pipe.getLastBufferedIndex() - receivedIndex - 1);
				}
			} // end of if (size > 0 && supportsSequence)
			// end of compacting pipe data
			if (firstResponse && r.req.pipeSequence > pipe.getSequence()) {
				pipe.setSequence(r.req.pipeSequence);
			}
			
			int size = list.size();
			if (size > 0) {
				// output data into string builder
				int startingIndex = pipe.getLastBufferedIndex();
				if (firstResponse || (supportsSequence && r.sentSequence == pipe.getSequence())) {
					startingIndex = 0;
				}
				int index = startingIndex;
				long lastSentSequence = -1;
				int lastSequenceIndex = -1;
				boolean endsWithSequence = false;
				boolean hasOutput = false;
				while (index < size) {
					SimpleSerializable ss = list.get(index); // peek at
					if (ss == null) {
						index++;
						break; // terminating signal
					}
					endsWithSequence = false;
					if (supportsSequence && ss instanceof SimplePipeSequence) {
						endsWithSequence = true;
						SimplePipeSequence sps = (SimplePipeSequence) ss;
						if (sps.sequence < r.sentSequence) {
							// Skip pipe sequence that is less than expected sequence
							index++;
							continue;
						}
						lastSentSequence = sps.sequence;
						lastSequenceIndex = index;
					}
					builder.append(SimplePipeUtils.output(type, key, ss.serialize(null,
							SimplePipeRequest.PIPE_TYPE_SCRIPT != type && SimplePipeRequest.PIPE_TYPE_XSS != type)));
					hasOutput = true;
					items++;
					if (live && maxItems > 0 && items >= maxItems && !isContinuum) {
						if (!supportsSequence || lastSentSequence > 0) {
							index++;
							break;
						} // else support sequence but no pipe sequence yet, load more
					}
					if (!isContinuum) {
						if (ss instanceof ISimplePipePriority) {
							ISimplePipePriority spp = (ISimplePipePriority) ss;
							int p = spp.getPriority();
							if (p <= 0) {
								p = ISimplePipePriority.IMPORTANT;
							}
							priority += p;
						} else {
							priority += ISimplePipePriority.IMPORTANT;
						}
					}
					index++;
				}
				
				if (live && !isScripting && !isContinuum
						&& priority < ISimplePipePriority.IMPORTANT
						&& now - beforeLoop < PipeConfig.queryTimeout
						&& (maxItems <= 0 || items < maxItems)) {
					// not reaching data flushing, ignore all peeked data and return
					return 100;
				} // else continue

				// mark data as being output
				boolean monitoring = false;
				if (supportsSequence) {
					// client side supports pipe sequence, pipe data is marked as OK in /u/p?k=######&t=n&r=# request
					// just mark data cached false
					for (int i = startingIndex; i < index; i++) {
						SimpleSerializable ss = list.get(i);
						if (ss instanceof ISimpleCacheable) {
							ISimpleCacheable sc = (ISimpleCacheable) ss;
							sc.setCached(false);
						}
					}
					// update indexes
					int updatedLastIndex = index;
					int updatedFirstIndex = -1;
					if (endsWithSequence && hasOutput) {
						if (r.sentSequence != lastSentSequence) {
							r.sentSequence = lastSentSequence;
						}
						if (index >= size) {
							index = size - 1;
						}
						updatedFirstIndex = index;
						updatedLastIndex = index + 1;
					} else if (hasOutput) { // not ends with sequence
						SimplePipeSequence seq = new SimplePipeSequence();
						int v = pipe.getSimpleVersion();
						if (v >= 202) {
							seq.setSimpleVersion(v);
						}
						r.sentSequence = Math.max(Math.max(r.sentSequence, lastSentSequence), pipe.getSequence()) + 1;
						seq.sequence = r.sentSequence;
						builder.append(SimplePipeUtils.output(type, key, seq.serialize(null, 
								SimplePipeRequest.PIPE_TYPE_SCRIPT != type && SimplePipeRequest.PIPE_TYPE_XSS != type)));
						if (index > size) {
							index = size;
						}
						list.add(index, seq);
						updatedFirstIndex = index;
						updatedLastIndex = index + 1;
					} else if (lastSequenceIndex > 0) { // has no output, but maybe just skip some pipe sequence objects
						updatedFirstIndex = lastSequenceIndex;
						updatedLastIndex = lastSequenceIndex + 1;
					}
					if (updatedFirstIndex > 0 && pipe.getFirstPipeSequenceIndex() == 0) {
						// update only if first sequence index is not initialized
						pipe.setFirstPipeSequenceIndex(updatedFirstIndex);
					}
					pipe.setLastBufferedIndex(updatedLastIndex);
				} else {
					// not supporting pipe sequence on client side, remove pipe data now
					monitoring = pipe.isMonitoringEvents();
					if (monitoring) {
						okEvts = new SimpleSerializable[index - startingIndex];
					}
					for (int i = startingIndex; i < index; i++) {
						SimpleSerializable ss = list.remove(0);
						if (ss instanceof ISimpleCacheable) {
							ISimpleCacheable sc = (ISimpleCacheable) ss;
							sc.setCached(false);
						}
						if (monitoring && !(ss instanceof SimplePipeSequence)) {
							okEvts[evtIdx] = ss;
							evtIdx++;
						}
					}
				}
			} // end of if (size > 0)
		} // end of synchronized (list)
		
		markPipeDataOK(pipe, okEvts, evtIdx);
		
		boolean pipeContinue = false;
		if (!live) {
	        //SimplePipeRunnable pipe = SimplePipeHelper.getPipe(key);
	        long waitClosingInterval = pipe == null ? 5000 : pipe.pipeWaitClosingInterval();
	        pipeContinue = now - lastLiveDetected < waitClosingInterval; // still in waiting interval
		} else {
			lastLiveDetected = now;
			if ((r.buffer.length() == 0 && now - beforeLoop >= PipeConfig.queryTimeout
					&& SimplePipeRequest.PIPE_TYPE_CONTINUUM != type)
					/*|| (lastPipeDataWritten > 0
						&& now - lastPipeDataWritten >= PipeConfig.queryTimeout
						&& (SimplePipeRequest.PIPE_TYPE_CONTINUUM == type
								|| SimplePipeRequest.PIPE_TYPE_SCRIPT == type))*/) {
				builder.append(SimplePipeUtils.output(type, key, SimplePipeRequest.PIPE_STATUS_OK));
			}
			
			boolean itemCountOK = PipeConfig.maxItemsPerQuery <= 0 || items < maxItems;
			pipeContinue = isContinuum
					|| (itemCountOK && now - beforeLoop < PipeConfig.queryTimeout // not query timeout
							&& priority < ISimplePipePriority.IMPORTANT) 
					|| (itemCountOK && now - beforeLoop < PipeConfig.pipeBreakout // not pipe breakout
							&& isScripting);
		}
		
		// Synchronize updates to PipeRequest
		if (builder.length() > 0) {
			r.buffer.append(builder);
		}
		r.items = items;
		r.priority = priority;
		r.lastLiveDetected = lastLiveDetected;
		return pipeContinue ? 100 : 200;
	}

	private static void markPipeDataOK(SimplePipeRunnable pipe,
			SimpleSerializable[] okEvts, int evtIdx) {
		if (evtIdx > 0) {
			if (evtIdx == okEvts.length) {
				pipe.pipeDataOK(okEvts);
			} else {
				SimpleSerializable[] evts = new SimpleSerializable[evtIdx];
				System.arraycopy(okEvts, 0, evts, 0, evtIdx);
				pipe.pipeDataOK(evts);
			}
		}
	}
	
	static void pipeEnd(StringBuffer buffer, String key, char type, long beforeLoop, int items) {
		SimplePipeRunnable pipe = SimplePipeHelper.getPipe(key);
		if (pipe == null || pipe.getPipeData() /*SimplePipeHelper.getPipeDataList(key)*/ == null
				|| !pipe.isPipeLive() /*!SimplePipeHelper.isPipeLive(key)*/) { // pipe is tore down!
			//SimplePipeHelper.notifyPipeStatus(key, false); // Leave for pipe monitor to destroy it
			SimplePipeHelper.removePipe(key);
			buffer.append(SimplePipeUtils.output(type, key, SimplePipeRequest.PIPE_STATUS_DESTROYED));
		} else if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type
				&& (System.currentTimeMillis() - beforeLoop >= PipeConfig.pipeBreakout
						|| (PipeConfig.maxItemsPerQuery > 0 && items >= PipeConfig.maxItemsPerQuery * 5))) {
			buffer.append(SimplePipeUtils.output(type, key, SimplePipeRequest.PIPE_STATUS_CONTINUE));
		}
		if (buffer.length() == 0) {
			buffer.append(SimplePipeUtils.output(type, key, SimplePipeRequest.PIPE_STATUS_OK));
		}
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { // iframe
			buffer.append("</body></html>");
		} 
	}

	protected static byte[] gzipDecompress(byte[] responseData, int offset, int length) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(length * 4); // for plain text, compress ratio is normally 25%.
		ByteArrayInputStream bais = new ByteArrayInputStream(responseData, offset, length);
		GZIPInputStream gis = null;
		boolean error = false;
		try {
			baos.write(new byte[] { 'W', 'L', 'L' });
			gis = new GZIPInputStream(bais);
			byte[] buffer = new byte[8096];
			int read = -1;
			while ((read = gis.read(buffer)) > 0) {
				baos.write(buffer, 0, read);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			error = true;
		} finally {
			if (gis != null) {
				try {
					gis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return error ? null : baos.toByteArray();
	}

	@SuppressWarnings("unchecked")
	private void service(final SimpleHttpRequest req, final HttpResponse resp,
			byte[] ss, final boolean restful) {
		if (ss != null && ss.length > 7 && ss[2] == 'Z' && ss[1] == 'L' && ss[0] == 'W') {
			// unzip ss into WLL... bytes
			byte[] zz = gzipDecompress(ss, 7, ss.length - 7); // WLZ#### (#:0-9A-Za-z, based 62, max 14776336 ~ 14M)
			if (zz != null) {
				ss = zz;
			}
		}
		SimpleSerializable ssObj = null;
		if (!restful) {
			ssObj = SimpleSerializable.parseInstance(ss);
			if (ssObj == null || ssObj == SimpleSerializable.ERROR
					|| ssObj == SimpleSerializable.UNKNOWN) {
				System.out.println("[ERROR query!]" + new String(ss));
				System.out.println("[ERROR UA]" + req.userAgent);
				System.out.println("[ERROR URL]" + req.url);
				System.out.println("[ERROR IP]" + req.remoteIP);
				System.out.println("[ERROR Referrer]" + req.referer);
				System.out.println("[ERROR Host]" + req.host);
				HttpWorkerUtils.send400Response(req, resp);
				HttpLoggingUtils.addLogging(req.host, req, 400, 0);
				errorRequests++;
				return;
			}
			try {
				ssObj.deserializeBytes(ss);
			} catch (Throwable e) {
				e.printStackTrace();
				HttpWorkerUtils.send400Response(req, resp);
				HttpLoggingUtils.addLogging(req.host, req, 400, 0);
				errorRequests++;
				return;
			}
		} else {
			Map<String, Object> properties = new HashMap<String, Object>();
			Object requstObj = req.requestData;
			String requestData = (requstObj == null) ? "" : (requstObj instanceof String ? (String) requstObj
			         : new String((byte[]) requstObj, HttpWorkerUtils.UTF_8));
			String[] datas = requestData.split("&");
			for (String prop : datas) {
				String[] propArray = prop.split("=");
				if (propArray.length != 2) continue;
				String value = null;
				try {
					value = URLDecoder.decode(propArray[1], "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				if (value == null) {
					continue;
				}
				String propName = propArray[0];
				if (propName.endsWith("]")) {
					int idx = propName.indexOf("[");
					if (idx == -1) {
						continue;
					}
					String indexStr = propName.substring(idx + 1, propName.length() - 1);
					int index = -1;
					if (indexStr.length() != 0) {
						try {
							index = Integer.parseInt(indexStr);
						} catch (NumberFormatException e) {
							index = 0 + indexStr.charAt(0);
						}
					}
					List<String> list = null;
					String name = propName.substring(0, idx);
					Object propValue = properties.get(name);
					if (propValue != null) {
						if (!(propValue instanceof List)) {
							continue;
						}
						list = (List<String>) propValue;
					} else {
						list = new ArrayList<String>();
						properties.put(name, list);
					}
					if (index >= 0) {
						for (int k = list.size(); k < index; k++) {
							list.add(k, "");
						}
						list.add(index, value);
					} else {
						list.add(value);
					}
				} else {
					properties.put(propName, value);
				}
			}
			ssObj = SimpleSerializable.parseInstance(properties);
			if (ssObj == null || ssObj == SimpleSerializable.ERROR
					|| ssObj == SimpleSerializable.UNKNOWN) {
				System.out.println("[ERROR query!]" + new String(ss));
				System.out.println("[ERROR UA]" + req.userAgent);
				System.out.println("[ERROR URL]" + req.url);
				System.out.println("[ERROR IP]" + req.remoteIP);
				System.out.println("[ERROR Referrer]" + req.referer);
				System.out.println("[ERROR Host]" + req.host);
				HttpWorkerUtils.send400Response(req, resp);
				HttpLoggingUtils.addLogging(req.host, req, 400, 0);
				errorRequests++;
				return;
			}
			
			try {
				ssObj.deserialize(properties);
			} catch (Throwable e) {
				e.printStackTrace();
				HttpWorkerUtils.send400Response(req, resp);
				HttpLoggingUtils.addLogging(req.host, req, 400, 0);
				errorRequests++;
				return;
			}
		}
		
		if (ssObj instanceof SimpleRPCRunnable) {
			final SimpleRPCRunnable runnable = (SimpleRPCRunnable) ssObj;
			if (runnable instanceof ISimpleRequestInfoBinding) {
				ISimpleRequestInfoBinding infoSetter = (ISimpleRequestInfoBinding) runnable;
				infoSetter.setRequestHost(req.host);
				infoSetter.setRequestURL(req.url);
				infoSetter.setReferer(req.referer);
				infoSetter.setRemoteUserAgent(req.userAgent);
				infoSetter.setRemoteIP(req.remoteIP);
				if (runnable instanceof ISimpleRequestBinding) {
					// TODO: Buggy port!
					int port = req.port == 0 ? (server.isSSLEnabled() ? 443 : 80) : req.port;
					ISimpleRequestBinding reqSetter = (ISimpleRequestBinding) runnable;
					reqSetter.setRequestPort(port);
					reqSetter.setRequestSecurity(server.isSSLEnabled());
				}
				//infoSetter.setLanguages(null);
			}
			if (runnable instanceof SimplePipeRunnable) {
				SimplePipeRunnable pipeRunnable = (SimplePipeRunnable) runnable;
				pipeRunnable.setPipeHelper(new SimplePipeHelper.IPipeThrough() {
				
					public void helpThrough(SimplePipeRunnable pipe,
							SimpleSerializable[] objs) {
						String pipeKey = pipe.pipeKey;
						if (pipeKey != null) {
							SimplePipeHelper.pipeIn(pipeKey, objs);
							monitor.notifyPipe(pipeKey);
						}
					}
				
				});
				//pipeRunnable.pipeManaged = true; //managingPipe;
			}
			if (runnable instanceof CompoundPipeSession) {
				CompoundPipeSession session = (CompoundPipeSession) runnable;
				SimplePipeRunnable pipe = SimplePipeHelper.getPipe(session.pipeKey);
				if (pipe instanceof CompoundPipeRunnable) {
					CompoundPipeRunnable p = (CompoundPipeRunnable) pipe;
					p.weave(session);
				}
			}

			if (!runnable.supportsGZipEncoding()) {
				req.supportGZip = false;
			}
			if (!runnable.supportsKeepAlive()) {
				req.keepAliveMax =  0;
				req.keepAliveTimeout = 0;
			}
			SimpleRPCRunnable clonedRunnable = null;
			if (runnable.supportsDeltaResponse()) {
				try {
					clonedRunnable = (SimpleRPCRunnable) runnable.clone();
				} catch (CloneNotSupportedException e) {
					//e.printStackTrace();
				}
			}
			if (runnable instanceof IRequestMonitor) {
				req.monitor = (IRequestMonitor) runnable;
			}
			
			boolean serverError = false;
			if (runnable instanceof ISimpleCometable) {
				ISimpleCometable cometRunnable = (ISimpleCometable) runnable;
				if (cometRunnable.supportsCometMode()) {
					try {
						final SimpleRPCRunnable runnableClone = clonedRunnable;
						CometRunnable cometTask = new CometRunnable() {
							
							@Override
							public void run() {
								boolean justResponsed = false;
								synchronized (this) { // cometTask
									if (!called) {
										called = true;
										req.lastSent = System.currentTimeMillis();
										req.comet = false;
										if (!headerSent) {
											headerSent = true;
											sendServiceResponse(req, resp, runnable, runnableClone, restful);
											req.monitor = null;
										} else {
											sendServiceChunkedData(req, resp, runnable, runnableClone, restful);
											req.monitor = null;
											if (closeSocket) {
												closeSockets.put(resp.socket, req);
											}
										}
										justResponsed = true;
									}
								}
								if (justResponsed) {
									chainingRequest(req, resp);
								}
							}
							
						};
						boolean comet = cometRunnable.cometRun(cometTask);
						req.comet = comet;
						if (!comet) {
							sendServiceResponse(req, resp, runnable, runnableClone, restful);
							req.monitor = null;
						} else if (cometRunnable.supportsChunkedEncoding()) {
							synchronized (cometTask) {
								if (!cometTask.headerSent) {
									// comet, sent chunked 0 header
									cometTask.closeSocket = sendServiceChunkedHeader(req, resp, restful);
									req.lastSent = System.currentTimeMillis();
									cometTask.headerSent = true;
								}
							}
						}
						return;
					} catch (Throwable e) {
						e.printStackTrace();
						serverError = true;
					}
				}
			}
			if (!serverError) { // not run in #cometRun
				try {
					runnable.ajaxRun();
				} catch (Throwable e) {
					e.printStackTrace();
					serverError = true;
				}
			}
			if (serverError) {
				runnable.ajaxFail();
				HttpWorkerUtils.send500Response(req, resp);
				HttpLoggingUtils.addLogging(req.host, req, 500, 0);
				req.monitor = null;
				return;
			}
			sendServiceResponse(req, resp, runnable, clonedRunnable, restful);
			req.monitor = null;
		} else {
			HttpWorkerUtils.send400Response(req, resp);
			HttpLoggingUtils.addLogging(req.host, req, 400, 0);
			//errorRequests++;
		}
	}

	boolean sendServiceChunkedHeader(final SimpleHttpRequest req, final HttpResponse resp, boolean restful) {
		StringBuilder responseBuilder = new StringBuilder(256);
		responseBuilder.append("HTTP/1.");
		responseBuilder.append(req.v11 ? '1' : '0');
		responseBuilder.append(" 200 OK\r\n");
		String serverName = HttpConfig.serverSignature;
		if (req.requestCount < 1 && serverName != null && serverName.length() > 0) {
			responseBuilder.append("Server: ").append(serverName).append("\r\n");
		}
		boolean closeSocket = HttpWorkerUtils.checkKeepAliveHeader(req, responseBuilder);
		
		if (req.requestID != null) {
			responseBuilder.append("Content-Type: text/javascript; charset=UTF-8\r\n");
		} else {
			if (restful) {
				//responseBuilder.append("Content-Type: text/plain; charset=UTF-8\r\n");
				responseBuilder.append("Content-Type: application/json\r\n");
			} else {
				responseBuilder.append("Content-Type: application/octet-stream\r\n");
			}
		}
		responseBuilder.append("Pragma: no-cache\r\nCache-Control: no-cache, no-transform\r\nTransfer-Encoding: chunked\r\n\r\n");
		req.lastSent = System.currentTimeMillis();
		server.send(resp.socket, responseBuilder.toString().getBytes());
		return closeSocket;
	}

	void sendServiceChunkedData(final SimpleHttpRequest req, final HttpResponse resp,
			SimpleRPCRunnable runnable, SimpleRPCRunnable clonedRunnable, boolean restful) {
		String serialize = null;
		SimpleFilter filter = null;
		if (runnable.supportsDeltaResponse()) {
			final Set<String> diffs = SimpleRPCUtils.compareDiffs(runnable, clonedRunnable);
			filter = new SimpleFilter() {
				
				public boolean accept(String field) {
					return diffs.contains(field);
				}
	
				public boolean ignoreDefaultFields() {
					return false;
				}
			
			};
		} else { // all fields are returned.
			filter = NO_DELTA_FILTER;
		}
		if (!restful) {
			serialize = runnable.serialize(filter);
		} else {
			serialize = runnable.jsonSerialize(filter, req.url.endsWith("/simplejson") ? "" : null);
		}

		String content = serialize;
		if (req.requestID != null) {
			StringBuilder builder = new StringBuilder(1024);
			builder.append("net.sf.j2s.ajax.SimpleRPCRequest.xssNotify(\"");
			builder.append(req.requestID);
			builder.append("\", \"");
			builder.append(serialize.replaceAll("\\\\", "\\\\\\\\")
					.replaceAll("\r", "\\\\r")
					.replaceAll("\n", "\\\\n")
					.replaceAll("\"", "\\\\\""));
			builder.append("\");");
			content = builder.toString();
		}
		if (content == null || content.length() == 0) {
			server.send(resp.socket, "0\r\n\r\n".getBytes());
			return;
		}
		String hexStr = Integer.toHexString(content.length());
		String output = hexStr + "\r\n" + content + "\r\n0\r\n\r\n";
		server.send(resp.socket, output.getBytes(restful ? HttpWorkerUtils.UTF_8 : HttpWorkerUtils.ISO_8859_1));
		req.lastSent = System.currentTimeMillis();
	}

	void sendServiceResponse(final SimpleHttpRequest req, final HttpResponse resp,
			SimpleRPCRunnable runnable, SimpleRPCRunnable clonedRunnable, boolean restful) {
		SimpleFilter filter = null;
		if (runnable.supportsDeltaResponse()) {
			final Set<String> diffs = SimpleRPCUtils.compareDiffs(runnable, clonedRunnable);
			filter = new SimpleFilter() {
				
				public boolean accept(String field) {
					return diffs.contains(field);
				}
	
				public boolean ignoreDefaultFields() {
					return false;
				}
			
			};
		} else { // all fields are returned.
			filter = NO_DELTA_FILTER;
		}
		StringBuilder responseBuilder = new StringBuilder(256);
		responseBuilder.append("HTTP/1.");
		responseBuilder.append(req.v11 ? '1' : '0');
		responseBuilder.append(" 200 OK\r\n");
		String serverName = HttpConfig.serverSignature;
		if (req.requestCount < 1 && serverName != null && serverName.length() > 0) {
			responseBuilder.append("Server: ").append(serverName).append("\r\n");
		}
		boolean closeSocket = HttpWorkerUtils.checkKeepAliveHeader(req, responseBuilder);
		
		byte[] outBytes = null;
		if (req.requestID != null) {
			String serialize = runnable.serialize(filter);
			StringBuilder builder = new StringBuilder(1024);
			builder.append("net.sf.j2s.ajax.SimpleRPCRequest.xssNotify(\"");
			builder.append(req.requestID);
			builder.append("\", \"");
			builder.append(serialize.replaceAll("\\\\", "\\\\\\\\")
					.replaceAll("\r", "\\\\r")
					.replaceAll("\n", "\\\\n")
					.replaceAll("\"", "\\\\\""));
			builder.append("\");");
			String output = builder.toString();
			
			byte[] bytes = output.getBytes();
			int length = bytes.length;
			boolean toGZip = length > HttpConfig.gzipStartingSize && req.supportGZip && HttpWorkerUtils.isUserAgentSupportGZip(req.userAgent);
			if (toGZip) {
				bytes = HttpWorkerUtils.gZipCompress(bytes);
				length = bytes.length;
				responseBuilder.append("Content-Encoding: gzip\r\n");
			}

			responseBuilder.append("Content-Type: text/javascript; charset=UTF-8\r\nContent-Length: ");
			responseBuilder.append(length);
			responseBuilder.append("\r\n\r\n");
			byte[] responseBytes = responseBuilder.toString().getBytes();
			outBytes = new byte[responseBytes.length + bytes.length];
			System.arraycopy(responseBytes, 0, outBytes, 0, responseBytes.length);
			System.arraycopy(bytes, 0, outBytes, responseBytes.length, bytes.length);
		} else {
			byte[] bytes = null;
			if (restful) {
				String serialize = runnable.jsonSerialize(filter, req.url.endsWith("/simplejson") ? "" : null);
				bytes = serialize.getBytes(HttpWorkerUtils.UTF_8);
			} else {
				bytes = runnable.serializeBytes(filter);
			}
			int length = bytes.length;
			boolean toGZip = length > HttpConfig.gzipStartingSize && req.supportGZip && HttpWorkerUtils.isUserAgentSupportGZip(req.userAgent);
			if (toGZip) {
				bytes = HttpWorkerUtils.gZipCompress(bytes);
				length = bytes.length;
				responseBuilder.append("Content-Encoding: gzip\r\n");
			}
			if (restful) {
				//responseBuilder.append("Content-Type: text/plain; charset=UTF-8\r\n");
				responseBuilder.append("Content-Type: application/json\r\n");
			} else {
				responseBuilder.append("Content-Type: application/octet-stream\r\n");
			}
			responseBuilder.append("Content-Length: ");
			responseBuilder.append(length);
			responseBuilder.append("\r\n\r\n");
			byte[] responseBytes = responseBuilder.toString().getBytes();
			outBytes = new byte[responseBytes.length + bytes.length];
			System.arraycopy(responseBytes, 0, outBytes, 0, responseBytes.length);
			System.arraycopy(bytes, 0, outBytes, responseBytes.length, bytes.length);
			
		}
		req.lastSent = System.currentTimeMillis();
		req.sending = outBytes.length;
		server.send(resp.socket, outBytes);
		if (closeSocket) {
			closeSockets.put(resp.socket, req);
		}
	}

}
