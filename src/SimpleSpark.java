import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SimpleSpark {
	private static final String host = "localhost";
	private static final int port = 8082;

	public static void main(String[] args) {
		// 模拟生产者产生消息
		Producer producer = new Producer();
		Map<String, String> content = null;
		while (true) {
			content = sendRequest("{\"action\":\"consume\",\"topic\":\"topic_bs\"}");

			if (content != null) {
				// 处理数据
				Map<String, String> result = processData(content);
				String jsonString = "{\"action\":\"produce\",\"topic\":\"topic_lbs\"}";

				// 将 Map 中的键值对拼接到 JSON 字符串中
				StringBuilder sb = new StringBuilder(jsonString);
				sb.insert(sb.length() - 1, ","); // 在倒数第二个字符位置插入逗号

				for (Map.Entry<String, String> entry : result.entrySet()) {
					sb.insert(sb.length() - 2, String.format(",\"%s\":\"%s\"", entry.getKey(), entry.getValue()));
				}
				sendRequest(sb.toString());
			} else {
				try {
					TimeUnit.SECONDS.sleep(1); // 模拟没有消息时的等待
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static Map<String, String> processData(Map<String, String> data) {
		// 模拟处理逻辑
		return data;
	}

	public static Map<String, String> sendRequest(String jsonRequest) {
		while (true) {
			try (Socket socket = new Socket(host, port);
				 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

				out.println(jsonRequest);
				System.out.println("Sent: " + jsonRequest);
				Map<String, String> response = jsonStringToMap(in.readLine());

				System.out.println("Received: " + response);

				// 判断响应状态是否成功
				if ("success".equals(response.get("status"))) {
					return response;
				} else {
					System.out.println("Response status is not successful, retrying...");
					Thread.sleep(1000);
				}

			} catch (Exception e) {
				System.out.println("fail to connect");
				return null;
			}
		}
	}

	public static Map<String, String> jsonStringToMap(String jsonString) {
		Map<String, String> map = new HashMap<>();

		if (jsonString != null && jsonString.length() > 0) {
			// 手动解析 JSON 字符串并转换为 Map
			String[] keyValuePairs = jsonString.substring(1, jsonString.length() - 1).split(",");
			for (String pair : keyValuePairs) {
				String[] entry = pair.split(":");
				map.put(entry[0].trim().replace("\"", ""), entry[1].trim().replace("\"", ""));
			}
		}
		return map;
	}
}

// 假设生产者接口
class Producer {
	public void produceMessage(String message) {
		// 模拟将处理结果发送到另一个消息队列
		System.out.println("Producing message: " + message);
	}
}

// 假设消费者接口
class Consumer {
	public String consumeMessage() {
		// 模拟从消息队列中获取消息
		return "Original message";
	}
}
