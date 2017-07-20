/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.pilet.HttpWorkerUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.j2s.ajax.ISimplePipePriority;
import net.sf.j2s.ajax.SimplePipeHelper;
import net.sf.j2s.ajax.SimplePipeRequest;
import net.sf.j2s.ajax.SimplePipeRunnable;

class SimplePipeMonitor implements Runnable {

	private PiledAbstractServer server;
	
	private SimpleHttpWorker[] workers;
	
	private Map<String, PipeRequest> pipeRequests = new ConcurrentHashMap<String, PipeRequest>();
	
	public SimplePipeMonitor(PiledAbstractServer server, HttpWorker[] workers) {
		super();
		this.server = server;
		this.workers = new SimpleHttpWorker[workers.length];
		for (int i = 0; i < workers.length; i++) {
			this.workers[i] = (SimpleHttpWorker) workers[i];
			this.workers[i].monitor = this;
		}
	}

	@Override
	public void run() {
		while (server.running) {
			long interval = PipeConfig.pipeCheckingInterval;
			if (interval <= 0) {
				interval = 500;
			}
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long now = System.currentTimeMillis();
			Set<String> toNotifyPipes = new HashSet<String>();
			for (PipeRequest req : pipeRequests.values()) {
				int items = req.items;
				int priority = req.priority;
				String key = req.key;
				long beforeLoop = req.beforeLoop;
				long lastLiveDetected = req.lastLiveDetected;
				char type = req.type;
				
				if (req.pipe == null) {
					req.pipe = SimplePipeHelper.getPipe(key);
				}
				SimplePipeRunnable pipe = req.pipe;
				long waitClosingInterval = 5000;
				if (pipe != null) {
					waitClosingInterval = pipe.pipeWaitClosingInterval();
				}
				
				if (pipe == null || !pipe.isPipeLive()) { //!SimplePipeHelper.isPipeLive(key)) {
					if (now - lastLiveDetected > waitClosingInterval) {
						toNotifyPipes.add(req.key);
						// break out while loop so pipe connection will be closed
						// to remove from timeout waiting queue?
						// call doPipe again and send out data
						//break;
					} else { // sleep 1s and continue to check pipe status again
						continue; // still needs waiting
					}
				} else {
					req.lastLiveDetected = now;
				}
				
				if (pipe != null && pipe.getPipeMode() == SimplePipeRequest.MODE_PIPE_CONTINUUM 
						&& pipe.checkCometStatus(now)) {
					req.buffer.append(pipe.pipeKey + SimplePipeRequest.PIPE_STATUS_OK);
				}

				int maxItems = SimplePipeRequest.PIPE_TYPE_SCRIPT == req.type ? 5 * PipeConfig.maxItemsPerQuery : PipeConfig.maxItemsPerQuery;
				if (pipe != null && pipe.getPipeData()/*SimplePipeHelper.getPipeDataList(key)*/ != null // may be broken down already!!
						&& (PipeConfig.maxItemsPerQuery <= 0 || items < maxItems
								|| SimplePipeRequest.PIPE_TYPE_CONTINUUM == type)
						&& (SimplePipeRequest.PIPE_TYPE_CONTINUUM == type
						|| (SimplePipeRequest.PIPE_TYPE_SCRIPT == type
								&& now - beforeLoop < PipeConfig.pipeBreakout)
						|| (priority < ISimplePipePriority.IMPORTANT
								&& now - beforeLoop < PipeConfig.queryTimeout))) {
					// still needs waiting
					if (req.buffer.length() > 0 && (SimplePipeRequest.PIPE_TYPE_CONTINUUM == type
							|| SimplePipeRequest.PIPE_TYPE_SCRIPT == type)) {
						toNotifyPipes.add(req.key); // to flush data!
					} else {
						continue;
					}
				} else {
					toNotifyPipes.add(req.key);
					// to remove from timeout waiting queue?
					// call doPipe again and send out data
				}
			}
			
			for (String key : toNotifyPipes) {
				notifyPipe(key);
			}
		}
	}
	
	public void monitor(PipeRequest req) {
		PipeRequest r = pipeRequests.put(req.key, req);
		if (r == req) {
			r = null;
		}
		if (r != null) { // Duplicated request detected
			// "r" is previous HTTP request for this pipe, end it
			SimpleHttpWorker.pipeEnd(r.buffer, r.key, r.type, r.beforeLoop, r.items);
			r.req.monitor = null;
			r.req.comet = false;
			if (r.headerSent) {
				String bufferedOutput = r.buffer.toString();
				SimplePipeUtils.pipeChunkedData(r.chunking, r.req, r.resp, bufferedOutput);
				int bufferedLength = bufferedOutput.length();
				if (bufferedLength > 0) {
					r.buffer.delete(0, bufferedLength);
					SimplePipeUtils.pipeChunkedData(r.chunking, r.req, r.resp, null);
//				} else {
//					System.out.println("Notify pipe with buffer output length = 0!");
				}
				if (r.closingSocket && r.resp.socket != null) {
					server.workers[r.resp.socket.hashCode() % server.workers.length].poolingRequest(r.resp.socket, r.req);
				}
			} else {
				HttpWorkerUtils.pipeOut(r.req, r.resp, r.contentType, null, r.buffer.toString(), false);
			}
			r.resp.worker.chainingRequest(r.req, r.resp);
		}
	}

	public void notifyPipe(String pipeKey) {
		PipeRequest r = pipeRequests.remove(pipeKey);
		if (r != null) {
			int pipedStatus = SimpleHttpWorker.doPipe(r, false);
			if (pipedStatus == 200) {
				SimpleHttpWorker.pipeEnd(r.buffer, r.key, r.type, r.beforeLoop, r.items);
				r.req.monitor = null;
				r.req.comet = false;
				String bufferedOutput = r.buffer.toString();
				if (r.headerSent) {
					int bufferedLength = bufferedOutput.length();
					SimplePipeUtils.pipeChunkedDataWithEnding(r.chunking, r.req, r.resp, bufferedOutput);
					if (bufferedLength > 0) {
						r.buffer.delete(0, bufferedLength);
					} else {
						System.out.println("Notify pipe with buffer output length = 0!");
					}
					if (r.closingSocket && r.resp.socket != null) {
						server.workers[r.resp.socket.hashCode() % server.workers.length].poolingRequest(r.resp.socket, r.req);
					}
				} else {
					HttpWorkerUtils.pipeOutBytes(r.req, r.resp, r.contentType, null, 
							bufferedOutput.getBytes(HttpWorkerUtils.ISO_8859_1), false);
				}
				r.resp.worker.chainingRequest(r.req, r.resp);
			} else {
				if (r.buffer.length() > 0 && (SimplePipeRequest.PIPE_TYPE_CONTINUUM == r.type
						|| SimplePipeRequest.PIPE_TYPE_SCRIPT == r.type)) {
					String bufferedOutput = r.buffer.toString();
					int bufferedLength = bufferedOutput.length();
					if (bufferedLength > 0) { // There are chances that r.buffer is modified
						// flush ...
						if (!r.headerSent) {
							r.closingSocket = SimplePipeUtils.pipeChunkedDataWithHeader(r.chunking, r.req, r.resp, r.contentType, false, bufferedOutput);
							r.headerSent = true;
						} else {
							SimplePipeUtils.pipeChunkedData(r.chunking, r.req, r.resp, bufferedOutput);
						}
						r.buffer.delete(0, bufferedLength);
					}
				}
				monitor(r);
			}
		}
	}

}
