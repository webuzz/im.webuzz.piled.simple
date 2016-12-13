/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.pilet.HttpConfig;
import im.webuzz.pilet.HttpRequest;
import im.webuzz.pilet.HttpResponse;
import im.webuzz.pilet.HttpWorkerUtils;
import im.webuzz.pilet.IPiledServer;

import java.util.Date;

import net.sf.j2s.ajax.SimplePipeRequest;

/**
 * Utilities for Simple RPC or Simple Pipe.
 * 
 * TODO: Rename class name to SimpleUtils
 * 
 * @author zhourenjian
 *
 */
class SimplePipeUtils {
	
	private final static String[] WEEK_DAYS_ABBREV = new String[] {
		"Sun", "Mon", "Tue", "Wed", "Thu",  "Fri", "Sat"
	};
	
	protected static String output(char type, String key, char evt) {
		StringBuilder builder = new StringBuilder();
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { 
			// iframe, so $ is a safe method identifier
			builder.append("<script type=\"text/javascript\">$ (\"");
		} else if (SimplePipeRequest.PIPE_TYPE_XSS == type) {
			builder.append("$p1p3p$ (\""); // $p1p3p$
		}
		builder.append(key);
		builder.append(evt);
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { // iframe
			builder.append("\");</script>\r\n");
		} else if (SimplePipeRequest.PIPE_TYPE_XSS == type) {
			builder.append("\");\r\n");
		}
		return builder.toString();
	}

	/**
	 * Output pipe data result in specified type.
	 * 
	 * @param type
	 * @param key String, pipe key
	 * @param str String pipe data
	 * @return Encoded pipe data restult 
	 */
	protected static String output(char type, String key, String str) {
		StringBuilder builder = new StringBuilder();
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { 
			// iframe, so $ is a safe method identifier
			builder.append("<script type=\"text/javascript\">$ (\"");
		} else if (SimplePipeRequest.PIPE_TYPE_XSS == type) {
			builder.append("$p1p3p$ (\""); // $p1p3p$
		}
		builder.append(key);
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type 
				|| SimplePipeRequest.PIPE_TYPE_XSS == type) {
			str = str.replaceAll("\\\\", "\\\\\\\\").replaceAll("\r", "\\\\r")
					.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\"");
			if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) {
				str = str.replaceAll("<\\/script>", "<\\/scr\" + \"ipt>");
			}
		}
		builder.append(str);
		if (SimplePipeRequest.PIPE_TYPE_SCRIPT == type) { // iframe
			builder.append("\");</script>\r\n");
		} else if (SimplePipeRequest.PIPE_TYPE_XSS == type) {
			builder.append("\");\r\n");
		}
		return builder.toString();
	}

	public static boolean checkKeepAliveHeader(HttpRequest req, StringBuilder responseBuilder) {
		if ((req.keepAliveMax > 1 && req.requestCount + 1 < req.keepAliveMax) || req.next != null) {
			responseBuilder.append("Keep-Alive: timeout=");
			responseBuilder.append(req.keepAliveTimeout);
			responseBuilder.append(", max=");
			responseBuilder.append(req.keepAliveMax);
			responseBuilder.append("\r\nConnection: keep-alive\r\n");
			return false;
		} else {
			responseBuilder.append("Connection: close\r\n");
			return true;
		}
	}

	@SuppressWarnings("deprecation")
	public static boolean pipeChunkedDataWithHeader(boolean chunking, HttpRequest request, HttpResponse response, String type, boolean cachable, String content) {
		boolean closeSocket = false;
		StringBuilder responseBuilder = new StringBuilder(512);
		responseBuilder.append("HTTP/1.");
		responseBuilder.append(request.v11 ? '1' : '0');
		responseBuilder.append(" 200 OK\r\n");
		String serverName = HttpConfig.serverSignature;
		if (request.requestCount < 1 && serverName != null && serverName.length() > 0) {
			responseBuilder.append("Server: ").append(serverName).append("\r\n");
		}
		if (!chunking) {
			responseBuilder.append("Connection: close\r\n");
			closeSocket = true;
		} else {
			closeSocket = checkKeepAliveHeader(request, responseBuilder);
		}

		if (!cachable) {
			Date date = new Date(System.currentTimeMillis() - 24L * 3600 * 1000);
			responseBuilder.append("Pragma: no-cache\r\nCache-Control: no-cache, no-transform\r\nExpires: ");
			responseBuilder.append(WEEK_DAYS_ABBREV[date.getDay()]);
			responseBuilder.append(", ");
			responseBuilder.append(date.toGMTString());
			responseBuilder.append("\r\n");
		}
		responseBuilder.append("Content-Type: ");
		responseBuilder.append(type);
		if (type.startsWith("text/")) {
			responseBuilder.append("; charset=UTF-8");
		}
		if (!chunking) {
			// responseBuilder.append("\r\nContent-Length: 1073741824\r\n\r\n"); // 1G, will be closed before reaching this number 
			responseBuilder.append("\r\n\r\n");
		} else {
			responseBuilder.append("\r\nTransfer-Encoding: chunked\r\n\r\n");
		}
		IPiledServer server = response.worker.getServer();
		if (!chunking) {
			if (content == null || content.length() == 0) {
				server.send(response.socket, responseBuilder.toString().getBytes());
				server.send(response.socket, HttpWorker.ZERO_BYTES);
				return closeSocket;
			}
			responseBuilder.append(content);
			server.send(response.socket, responseBuilder.toString().getBytes(HttpWorkerUtils.ISO_8859_1));
			return closeSocket;
		}
		if (content == null || content.length() == 0) {
			responseBuilder.append("0\r\n\r\n");
			server.send(response.socket, responseBuilder.toString().getBytes());
			return closeSocket;
		}
		String hexStr = Integer.toHexString(content.length());
		responseBuilder.append(hexStr);
		responseBuilder.append("\r\n");
		responseBuilder.append(content);
		responseBuilder.append("\r\n");
		server.send(response.socket, responseBuilder.toString().getBytes(HttpWorkerUtils.ISO_8859_1));
		return closeSocket;
	}
	
	public static void pipeChunkedDataWithEnding(boolean chunking, HttpRequest request, HttpResponse response, String content) {
		IPiledServer server = response.worker.getServer();
		if (!chunking) {
			if (content != null && content.length() != 0) {
				server.send(response.socket, content.getBytes(HttpWorkerUtils.ISO_8859_1));
			}
			server.send(response.socket, HttpWorker.ZERO_BYTES);
			return;
		}
		if (content == null || content.length() == 0) {
			server.send(response.socket, "0\r\n\r\n".getBytes());
			return;
		}
		String hexStr = Integer.toHexString(content.length());
		String output = hexStr + "\r\n" + content + "\r\n0\r\n\r\n";
		server.send(response.socket, output.getBytes(HttpWorkerUtils.ISO_8859_1));
	}

	public static void pipeChunkedData(boolean chunking, HttpRequest request, HttpResponse response, String content) {
		IPiledServer server = response.worker.getServer();
		if (!chunking) {
			if (content == null || content.length() == 0) {
				server.send(response.socket, HttpWorker.ZERO_BYTES);
				return;
			}
			server.send(response.socket, content.getBytes(HttpWorkerUtils.ISO_8859_1));
			return;
		}
		if (content == null || content.length() == 0) {
			server.send(response.socket, "0\r\n\r\n".getBytes());
			return;
		}
		String hexStr = Integer.toHexString(content.length());
		String output = hexStr + "\r\n" + content + "\r\n";
		server.send(response.socket, output.getBytes(HttpWorkerUtils.ISO_8859_1));
		return;
	}

}
