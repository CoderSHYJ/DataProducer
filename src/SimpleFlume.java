import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class SimpleFlume {
	public static void main(String[] args) {
		// 创建Channel组件
		Channel channel = new Channel();

		// 创建Source组件并启动
		Thread sourceThread = new Thread(new Source(channel));
		sourceThread.start();

		// 创建Sink组件并启动
		Thread sinkThread = new Thread(new Sink(channel));
		sinkThread.start();
	}
}

class Event {
	private Map<String, String> content;

	public Map<String, String> getContent() {
		return content;
	}

	public void setContent(Map<String, String> content) {
		this.content = content;
	}
}


class Channel {
	private LinkedBlockingQueue<Event> queue;

	public Channel() {
		this.queue = new LinkedBlockingQueue<>();
	}

	public void put(Event event) {
		queue.offer(event);
		//System.out.println("Put: channel has : "+queue.size());
	}

	public Event take() {
		//System.out.println("Take: channel has : "+queue.size());
		return queue.poll();
	}
}

class Source implements Runnable {
	private final String[] baseStationIds = {"lac123", "lac456"};
	private final String[] cellIds = {"cell123"};
	private final String[] customerNumbers = {"13800000000", "13800000770"};
	private final String[] eventDefIds = {"2"};
	private final String[] eventTimestamps = {"string"};
	private Channel channel;
	private Random rand;

	public Source(Channel channel) {
		this.channel = channel;
		this.rand = new Random();
	}

	@Override
	public void run() {
		int max=10,tmp=0;
		try {
			while (true) {
				Event event = new Event();
				Map<String, String> content = new HashMap<>();
				content.put("baseStationId", baseStationIds[rand.nextInt(baseStationIds.length)]);
				content.put("cellId", cellIds[rand.nextInt(cellIds.length)]);
				content.put("customerNumber", customerNumbers[rand.nextInt(customerNumbers.length)]);
				content.put("eventDefId", eventDefIds[rand.nextInt(eventDefIds.length)]);
				content.put("eventTimestamp", eventTimestamps[rand.nextInt(eventTimestamps.length)]);
				event.setContent(content);
				channel.put(event);
				Thread.sleep(1000);  // 每秒产生一个Event
				tmp++;
				if(tmp==max){
					//return;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class Sink implements Runnable {
	private Channel channel;
	private final String host="localhost";
	private final int port=8082;

	public Sink(Channel channel) {
		this.channel = channel;
	}

	@Override
	public void run() {
		while (true) {
			Event event = null;
			while (event == null) {
				event = channel.take();
			}
			System.out.println("receive: " + event.getContent());
			Map<String, String> content = event.getContent();
			String jsonString = "{\"action\":\"produce\",\"topic\":\"topic_bs\"}";

			// 将 Map 中的键值对拼接到 JSON 字符串中
			StringBuilder sb = new StringBuilder(jsonString);
			sb.insert(sb.length() - 1, ","); // 在倒数第二个字符位置插入逗号

			for (Map.Entry<String, String> entry : content.entrySet()) {
				sb.insert(sb.length() - 2, String.format(",\"%s\":\"%s\"", entry.getKey(), entry.getValue()));
			}
			sendRequest(sb.toString());
			//sendRequest("{\"action\":\"produce\",\"topic\":\"testTopic\",\"key\":\"key1\",\"value\":\"value1\"}");
		}
	}
	public void sendRequest(String jsonRequest) {
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
					return;
				} else {
					System.out.println("Response status is not successful, retrying...");
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				System.out.println("fail to connect");
			}
		}
	}
	public Map<String, String> jsonStringToMap(String jsonString) {
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
