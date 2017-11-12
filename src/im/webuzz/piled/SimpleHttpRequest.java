package im.webuzz.piled;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import im.webuzz.pilet.HttpQuickResponse;
import im.webuzz.pilet.HttpRequest;
import net.sf.j2s.ajax.SimplePipeRequest;
import net.sf.j2s.ajax.SimpleSerializable;

public class SimpleHttpRequest extends HttpRequest {

	/*
	 * For cross site scripting (XSS), we need to link different HTTP
	 * requests into one request based on session.
	 */
	static Map<String, SimpleHttpRequest> allSessions = new ConcurrentHashMap<String, SimpleHttpRequest>();

	/*
	 * For Java2Script XSS requests
	 */
	private String[] jzn;
	private long jzt;
	public String requestID;
	
	/*
	 * For Simple Pipe
	 */
	public String pipeKey;
	public char pipeType;
	public long pipeLastNotified;
	public long pipeSequence;
	public boolean pipeSwitching;

	public int simpleStatus; // 0: initialized, 1: received, 2: (>3) checked: simple, -2: (>3) checked: http  
	
	public void debugPrint() {
		super.debugPrint();
		System.out.println("requestID: " + requestID);
		System.out.println("jzn: " + jzn);
		if (jzn != null) {
			for (int i = 0; i < jzn.length; i++) {
				System.out.println(i + ": " + (jzn[i] != null ? jzn[i].length() : 0));
			}
		}
		System.out.println("jzt: " + jzt);		
	}
	
	public void reset() {
		super.reset();
		requestID = null;
		pipeKey = null;
		pipeType = 0;
		pipeLastNotified = -1;
		pipeSwitching = false;
		jzn = null;
		jzt = 0;
	}
	
	public HttpRequest clone(){
		SimpleHttpRequest r = new SimpleHttpRequest();
		cloneTo(r);
		r.requestID = requestID;
		r.pipeKey = pipeKey;
		r.pipeType = pipeType;
		r.pipeLastNotified = pipeLastNotified;
		r.pipeSwitching = pipeSwitching;
		r.jzn = jzn;
		r.jzt = jzt;
		return r;
	}
	
	private HttpQuickResponse generateXSSErrorResponse() {
		return new HttpQuickResponse("text/javascript", "net.sf.j2s.ajax.SimpleRPCRequest" +
				".xssNotify(\"" + requestID + "\", \"error\");");
	}

	@Override
	protected int checkParseRequest(StringBuilder request) {
		/*
		 * may be start with "jz[n|p|c|z]=", normal request may start with 
		 * raw "WLL100" or other tokens but charAt(3) should never be '='.
		 */
		if (request != null && request.length() > 3 && request.charAt(3) == '=') { // simplerpc?jzn=604107&jzp=1&jzc=1&jzz=WLL100...
			// parse XSS query
			int length = request.length();
			
			int jzzStarted = -1;
			int jzzStopped = -1;
			
			int jznStarted = -1;
			int jznStopped = -1;
			
			int jzpStarted = -1;
			int jzpStopped = -1;
			
			int jzcStarted = -1;
			int jzcStopped = -1;
			
			int unknownStarted = -1;
			int unknownStopped = -1;

			char c3 = request.charAt(0);
			char c2 = request.charAt(1);
			char c1 = request.charAt(2);
			for (int i = 3; i < length; i++) {
				char c0 = request.charAt(i);
				if (jznStarted != -1) {
					if (jznStopped == -1) {
						if (c0 == '&') { // got jzn=...
							jznStopped = i;
						}
						continue;
					}
				}
				if (jzpStarted != -1) {
					if (jzpStopped == -1) {
						if (c0 == '&') { // got jzp=...
							jzpStopped = i;
						}
						continue;
					}
				}
				if (jzcStarted != -1) {
					if (jzcStopped == -1) {
						if (c0 == '&') { // got jzc=...
							jzcStopped = i;
						}
						continue;
					}
				}
				if (jzzStarted != -1) {
					if (jzzStopped == -1) {
						if (c0 == '&') { // got jzz=
							jzzStopped = i;
						}
						continue;
					}
				}
				if (unknownStarted != -1) {
					if (unknownStopped == -1) {
						if (c0 == '&') { // gotcha
							unknownStopped = i;
							unknownStarted = -1;
						}
						continue;
					}
				}
				if (c0 == '=' && c2 == 'z' && c3 == 'j') { // jz?=
					if (c1 == 'n') {
						jznStarted = i + 1;
						jznStopped = -1;
					} else if (c1 == 'p') {
						jzpStarted = i + 1;
						jzpStopped = -1;
					} else if (c1 == 'c') {
						jzcStarted = i + 1;
						jzcStopped = -1;
					} else if (c1 == 'z') {
						jzzStarted = i + 1;
						jzzStopped = -1;
					} else {
						unknownStarted = i + 1;
						unknownStopped = -1;
					}
					c2 = 0;
					c1 = 0;
					c0 = 0;
				} else if (c0 == '=') {
					unknownStarted = i + 1;
					unknownStopped = -1;
					c2 = 0;
					c1 = 0;
					c0 = 0;
				}
				c3 = c2;
				c2 = c1;
				c1 = c0;
			}
			
			String jzzRequest = null;
			if (jzzStarted != -1) {
				if (jzzStopped == -1) {
					jzzStopped = length;
				}
				jzzRequest = request.substring(jzzStarted, jzzStopped);
			}

			// jzz without jzn is considered as XHR requests!
			if (jzzRequest == null || jzzRequest.trim().length() == 0) {
				//response = generateXSSErrorResponse();
				requestData = request.toString();
				return -1; // error
			}
			
			if (jznStarted != -1) {
				if (jznStopped == -1) {
					jznStopped = length;
				}
				requestID = request.substring(jznStarted, jznStopped);
			}
			if (requestID != null && requestID.length() != 0) {
				// when jzn is defined, it's considered as a script request!
				
				// make sure that servlet support cross site script request
				// always support cross site script requests
				if (!PipeConfig.supportXSS) {
					response = new HttpQuickResponse("text/javascript", "net.sf.j2s.ajax.SimpleRPCRequest" +
							".xssNotify(\"" + requestID + "\", \"unsupported\");");
					return -1; // error
				}
					
				// check script request counts
				String count = null;
				if (jzpStarted != -1) {
					if (jzpStopped == -1) {
						jzpStopped = length;
					}
					count = request.substring(jzpStarted, jzpStopped);
				}
				int partsCount = 1;
				if (count != null) {
					boolean formatError = false;
					try {
						partsCount = Integer.parseInt(count);
					} catch (NumberFormatException e) {
						formatError = true; 
					}
					if (formatError || partsCount <= 0) {
						response = generateXSSErrorResponse();
						return -1; // error
					}
				}
				if (partsCount != 1) {
					// check whether servlet can deal the requests
					if (partsCount > PipeConfig.maxXSSParts) {
						response = new HttpQuickResponse("text/javascript", "net.sf.j2s.ajax.SimpleRPCRequest" +
								".xssNotify(\"" + requestID + "\", \"exceedrequestlimit\");");
						return -1; // error
					}
					// check curent request index
					String current = null;
					if (jzcStarted != -1) {
						if (jzcStopped == -1) {
							jzcStopped = length;
						}
						current = request.substring(jzcStarted, jzcStopped);
					}
					int curPart = 1;
					if (current != null) {
						boolean formatError = false;
						try {
							curPart = Integer.parseInt(current);
						} catch (NumberFormatException e) {
							formatError = true;
						}
						if (formatError || curPart > partsCount) {
							response = generateXSSErrorResponse();
							return -1; // error
						}
					}
					long now = System.currentTimeMillis();
					for (Iterator<SimpleHttpRequest> itr = allSessions.values().iterator(); itr.hasNext();) {
						SimpleHttpRequest r = (SimpleHttpRequest) itr.next();
						if (now - r.jzt > PipeConfig.maxXSSLatency) {
							itr.remove();
						}
					}

					String[] parts = null;
					
					SimpleHttpRequest sessionRequest = null;
					// store request in session before the request is completed
					if (session == null) {
						do {
							StringBuilder builder = new StringBuilder(32);
							for (int i = 0; i < 32; i++) {
								int r = (int) Math.round((float) Math.random() * 61.1); // 0..61, total 62 numbers
								if (r < 10) {
									builder.append((char) (r + '0'));
								} else if (r < 10 + 26) {
									builder.append((char) ((r - 10) + 'a'));
								} else {
									builder.append((char) ((r - 10 - 26) + 'A'));
								}
							}
							session = builder.toString();
						} while (allSessions.get(session) != null);
						sessionRequest = new SimpleHttpRequest();
						allSessions.put(session, sessionRequest);
					} else {
						sessionRequest = allSessions.get(session);
						if (sessionRequest == null) {
							sessionRequest = new SimpleHttpRequest();
							allSessions.put(session, sessionRequest);
						}
					}
					
					if (sessionRequest.jzn == null) {
						sessionRequest.jzt = now;
						parts = new String[partsCount];
						sessionRequest.jzn = parts;
					} else {
						parts = sessionRequest.jzn;
						if (partsCount != parts.length) {
							response = generateXSSErrorResponse();
							return -1; // error
						}
					}
					parts[curPart - 1] = jzzRequest;
					for (int i = 0; i < parts.length; i++) {
						if (parts[i] == null) {
							// not completed yet! just response and wait next request.
							response = new HttpQuickResponse("text/javascript", "net.sf.j2s.ajax.SimpleRPCRequest" +
									".xssNotify(\"" + requestID + "\", \"continue\"" +
									((curPart == 1) ? ", \"" + session + "\");" : ");"));
							break;
						}
					}
					if (response != null) {
						return -1;
					}

					allSessions.remove(session);
					
					StringBuilder builder = new StringBuilder(16192);
					for (int i = 0; i < parts.length; i++) {
						builder.append(parts[i]);
						parts[i] = null;
					}
					request.delete(0, request.length()).append(builder.toString());
				} else {
					request.delete(0, request.length()).append(jzzRequest);
				}
			} else {
				request.delete(0, request.length()).append(jzzRequest); // no request id, normal request
			}
		} // end of XSS script query parsing
		return 0;
	}
	
	protected int fromBase62Length(char c) {
		if ('0' <= c && c <= '9') return c - '0';
		if ('A' <= c && c <= 'Z') return c - 'A' + 10;
		if ('a' <= c && c <= 'z') return c - 'a' + 10 + 26;
		return -1;
	}
	
	public HttpQuickResponse parseData(byte[] data) {
		if (simpleStatus == -2) {
			return super.parseData(data);
		}
		if (data == null || data.length == 0) {
			return new HttpQuickResponse(100); // Continue reading...
		}
		if (simpleStatus == 0 || simpleStatus == 1) {
			simpleStatus = 1; // received
			int pendingLength = pending == null ? 0 : pending.length;
			if (pendingLength == 0 && data[0] != 'W') {
				simpleStatus = -2; // http
				return super.parseData(data);
			}
			if (pendingLength + data.length >= 2) {
				if (pendingLength <= 1 && data[1 - pendingLength] != 'L') {
					simpleStatus = -2; // http
					return super.parseData(data);
				}
				if (pendingLength + data.length >= 3) {
					if (pendingLength <= 2 && data[2 - pendingLength] != 'L' && data[2 - pendingLength] != 'Z') {
						simpleStatus = -2; // http
						return super.parseData(data);
					} else {
						simpleStatus = 2; // simple
						// continue
					}
				}
			}
			if (simpleStatus != 2) {
				return super.parseData(data);
			}
		} // simpleStatus <= 1 : 0, 1
		
		// simple status is 2 now
		if (pending == null) {
			pending = new byte[data.length];
		} else {
			byte[] newData = new byte[dataLength + data.length];
			System.arraycopy(pending, 0, newData, 0, dataLength);
			pending = newData;
		}
		System.arraycopy(data, 0, pending, dataLength, data.length);
		if (pending[2] == 'Z') { // WLZ: gzip data
			if (pending.length < 7) {
				return new HttpQuickResponse(100);
			}
			int c1 = fromBase62Length((char) pending[3]);
			int c2 = fromBase62Length((char) pending[4]);
			int c3 = fromBase62Length((char) pending[5]);
			int c4 = fromBase62Length((char) pending[6]);
			if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0) {
				return new HttpQuickResponse(400);
			}
			int length = (((c1 * 62 + c2) * 62) + c3) * 62 + c4;
			int fullLength = 7 + length;
			if (pending.length < fullLength) {
				return new HttpQuickResponse(100);
			}
			if (pending.length > fullLength) {
				requestData = new byte[fullLength];
				System.arraycopy(pending, 0, requestData, 0, fullLength);
				dataLength = pending.length - fullLength;
				byte[] newPending = new byte[dataLength];
				System.arraycopy(pending, 0, newPending, 0, dataLength);
				pending = newPending;
			} else {
				requestData = pending;
				dataLength = 0;
				pending = null;
			}
		} else {
			SimpleSerializable ss = SimpleSerializable.parseInstance(pending);
			if (ss == null) {
				return new HttpQuickResponse(100);
			}
			int next = SimplePipeRequest.restBytesIndex(pending, 0);
			if (next <= 0) {
				return new HttpQuickResponse(100);
			}
			if (pending.length > next) {
				requestData = new byte[next];
				System.arraycopy(pending, 0, requestData, 0, next);
				dataLength = pending.length - next;
				byte[] newPending = new byte[dataLength];
				System.arraycopy(pending, 0, newPending, 0, dataLength);
				pending = newPending;
			} else {
				requestData = pending;
				dataLength = 0;
				pending = null;
			}
		}
		// continue
		url = "/r";
		method = "POST";
		response = new HttpQuickResponse(200);
		return response;
	}
	
}
