/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import java.util.Properties;

import net.sf.j2s.ajax.SimplePipeHelper;

/**
 * Configuration for Simple RPC and Simple Pipe.
 * 
 * @author zhourenjian
 * @see im.webuzz.config.Config
 */
public class SimpleConfig {

	/**
	 * Support cross site scripting(XSS) or not. If you want to support
	 * other web sites to call your services using Java2Script Simple RPC,
	 * enable this feature.
	 * 
	 * This feature is only useful for browsers.
	 */
	public static boolean supportXSS = true;
	/**
	 * How many XSS parts is supported. XSS Simple RPC is submitted by
	 * SCRIPT element's src attribute. As it is limited by URL length
	 * supported by server, Java2Script will split big RPC data into
	 * different XSS parts. If data is too large, it may require many
	 * XSS parts (SCRIPT src loadings). 128 XSS parts might be 1M data
	 * for some browsers.
	 */
	public static int maxXSSParts = 128;
	/**
	 * Server need to cache XSS data in memory. If following XSS parts
	 * fails to come, server should discard data in given time.
	 */
	public static long maxXSSLatency = 60000L; // 60s
	
	/**
	 * If there is no data or existed data is not important enough to
	 * flush to client, the pipe will keep it util it times out. On timeout
	 * existed data will be flushed or PIPE_STATUS_OK will be flushed. 
	 */
	public static long queryTimeout = 20000; // 20 seconds
	/**
	 * For continuum pipe, we still make a pipe break on a long time
	 * interval. Browsers might keep all open connection's content in
	 * memory. And after a long time, it will consume a lot of memory
	 * keeping those expired data. We break the pipe, and then re-connect
	 * the pipe to release those memory.
	 */
	public static long pipeBreakout = 1200000; // 20 minutes
	/**
	 * To avoid sending a lot of data in one response which may freeze
	 * browser, we set limit on how many items can be piped through in 
	 * one query.
	 * This feature only works on PIPE_QUERY mode.
	 */
	public static int maxItemsPerQuery = 100; // -1; // infinite
	
	/**
	 * Use "Transfer-Encoding: chunked" for piping data.
	 */
	public static boolean pipeChunking = true;
	
	/**
	 * Support continuum pipe switching or not. Query mode pipes supports
	 * pipe switching by nature.
	 */
	public static boolean pipeSwitching = true;
	
	/**
	 * Maximum of continuum pipe switching interval.
	 */
	public static long pipeSwitchingMaxInterval = 25000;
	
	/**
	 * Supports Simple RPC inside or not.
	 */
	public static boolean simpleRPCSupported = true;
	
	/**
	 * Supports Simple Pipe inside or not.
	 */
	public static boolean simplePipeSupported = true;
	
	/**
	 * Checking interval for pipe monitor.
	 * @see SimplePipeMonitor
	 */
	public static long pipeCheckingInterval = 500;

	/**
	 * Support logging for SimpleRPC or not.
	 */
	public static boolean rpcLogging = true;
	/**
	 * Support logging for SimplePipe or not.
	 */
	public static boolean pipeLogging = false;
	
	public static void update(Properties prop) {
		String p = prop.getProperty("maxItemsPerQuery");
		if (p != null && p.length() > 0) {
			int maxItems = 100;
			try {
				maxItems = Integer.parseInt(p);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			if (maxItems > 5) {
				SimplePipeHelper.MAX_ITEMS_PER_QUERY = maxItems;
			}
		}
	}

}
