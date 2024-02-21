import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
class Message {
	Map<String,String>content;
	public Message(Map<String,String>content) {
		this.content = content;
	}
	public Message(String content) {
		this.content = parseRequest(content);
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

class Topic {
	String name; // 主题名称
	LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>(); // 存储消息的列表

	public Topic(String name) {
		this.name = name;
	}

	public synchronized void produce(Message message) {
		try {
			messages.put(message);
		} catch (InterruptedException ignored) {
		}
	}

	public synchronized Message consume() {
		try {
			if (!messages.isEmpty()) {
				return messages.take();
			}
			return null;
		} catch (InterruptedException e) {
			return null;
		}
	}
}

public class SimpleKafka {
	private final int port = 8082; // 服务器监听端口
	private final Map<String, Topic> topics = new HashMap<>();

	public void start() throws IOException {
		ServerSocket serverSocket = new ServerSocket(port);
		System.out.println("Server started on port: " + port);

		while (true) {
			Socket clientSocket = serverSocket.accept();
			// 创建新的线程来处理客户端请求
			Thread clientHandlerThread = new Thread(() -> handleClient(clientSocket));
			clientHandlerThread.start();
		}
	}

	private void handleClient(Socket clientSocket) {
		try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

			String requestLine = in.readLine();
			if (requestLine != null) {
				System.out.println("Received: " + requestLine);
				Map<String, String> request = parseRequest(requestLine);

				switch (request.get("action")) {
					case "createTopic":
						String topicName = request.get("topic");
						topics.putIfAbsent(topicName, new Topic(topicName));
						out.println("{\"status\":\"success\"}");
						break;
					case "produce":
						Topic topic = topics.get(request.get("topic"));
						if (topic != null) {
							request.remove("action");
							request.remove("topic");
							topic.produce(new Message(request));
							out.println("{\"status\":\"success\"}");
						} else {
							out.println("{\"status\":\"error\",\"message\":\"Topic not found\"}");
						}
						break;
					case "consume":
						Topic consumeTopic = topics.get(request.get("topic"));
						if (consumeTopic != null) {
							Message message = consumeTopic.consume();
							if (message != null) {
								String jsonString = "{\"status\":\"success\"}";

								// 将 Map 中的键值对拼接到 JSON 字符串中
								StringBuilder sb = new StringBuilder(jsonString);
								sb.insert(sb.length() - 1, ","); // 在倒数第二个字符位置插入逗号

								for (Map.Entry<String, String> entry : message.content.entrySet()) {
									sb.insert(sb.length() - 2, String.format(",\"%s\":\"%s\"", entry.getKey(), entry.getValue()));
								}
								out.println(sb.toString());
							} else {
								out.println("{\"status\":\"error\",\"message\":\"No message available\"}");
							}
						} else {
							out.println("{\"status\":\"error\",\"message\":\"Topic not found\"}");
						}
						break;
					default:
						out.println("{\"status\":\"error\",\"message\":\"Invalid action\"}");
						break;
				}
				clientSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
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
	public static void main(String[] args) throws IOException {
		new SimpleKafka().start();
	}
}
