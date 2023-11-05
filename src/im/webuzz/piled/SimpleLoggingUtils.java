package im.webuzz.piled;

import java.util.Set;

import im.webuzz.pilet.HttpLoggingConfig;
import im.webuzz.pilet.HttpLoggingUtils;
import im.webuzz.pilet.HttpRequest;
import im.webuzz.pilet.HttpResponse;

public class SimpleLoggingUtils {

	public static void addLogging(String host, HttpRequest req, HttpResponse resp, String clazzName, int responseCode, long responseLength) {
		Set<String> ignorings = HttpLoggingConfig.ignoringHosts;
		if (ignorings != null && host != null && ignorings.contains(host)) {
			return;
		}
		StringBuilder builder = new StringBuilder(256);
		builder.append(req.method);
		builder.append(" ");
		builder.append(req.url);
		if (clazzName != null && clazzName.length() > 0) {
			builder.append("?");
			builder.append(clazzName);
		} else if (req.requestQuery != null && req.requestQuery.length() > 0) {
			builder.append("?");
			builder.append(req.requestQuery);
		}

		if (req.v11) {
			builder.append(" HTTP/1.1");
		} else {
			builder.append(" HTTP/1.0");
		}
		HttpLoggingUtils.addLogging(host, req, resp, builder.toString(), responseCode, responseLength);
	}

}
