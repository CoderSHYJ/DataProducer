import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SimpleClient {
	private final String host;
	private final int port;

	public SimpleClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public static void sendMapData(Map<String, String> dataMap) {
		try {
			String apiUrl = "http://localhost:8080/emit_dynamic_act";
			URL url = new URL(apiUrl);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setDoOutput(true);

			// 构建JSON格式的请求体
			StringBuilder jsonBody = new StringBuilder();
			jsonBody.append("{");
			for (Map.Entry<String, String> entry : dataMap.entrySet()) {
				jsonBody.append("\"").append(entry.getKey()).append("\":\"");
				jsonBody.append(entry.getValue()).append("\",");
			}
			jsonBody.deleteCharAt(jsonBody.length() - 1); // 移除最后一个逗号
			jsonBody.append("}");

			// 发送数据
			try (OutputStream os = connection.getOutputStream()) {
				System.out.println("URL "+jsonBody.toString());
				byte[] input = jsonBody.toString().getBytes("utf-8");
				os.write(input, 0, input.length);
			}

			// 获取响应
			int responseCode = connection.getResponseCode();
			System.out.println("Response code: " + responseCode);

			connection.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient("localhost", 8082);

		// 创建Topic
		client.sendRequest("{\"action\":\"createTopic\",\"topic\":\"topic_bs\"}");
		client.sendRequest("{\"action\":\"createTopic\",\"topic\":\"topic_lbs\"}");

		while(true){

			Map<String, String> message=client.sendRequest("{\"action\":\"consume\",\"topic\":\"topic_lbs\"}");
			if (message==null){
				try {
					//System.out.println("wait...");
					TimeUnit.SECONDS.sleep(5); // 模拟没有消息时的等待
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else {
				sendMapData(message);
				System.out.println(message);
			}
		}
	}

	public Map<String, String> sendRequest(String jsonRequest) {
		try (Socket socket = new Socket(host, port);
			 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

			out.println(jsonRequest);
			//System.out.println("Sent: " + jsonRequest);
			Map<String, String> response=parseRequest(in.readLine());
			System.out.println(response);
			if(!Objects.equals(response.get("status"), "success")){
				System.out.println("fail message");
				return null;
			}
			return response;
		} catch (Exception e) {
			System.out.println("fail to connect");
			return null;
		}
	}
	public Map<String, String> parseRequest(String jsonString) {
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
