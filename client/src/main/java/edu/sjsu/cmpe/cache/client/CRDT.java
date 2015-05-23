package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CRDT {

	private List<CacheServiceInterface> serverList;
	private CountDownLatch countDownLatch;

	public CRDT() {

		String[] server = { "http://localhost:3000","http://localhost:3001", "http://localhost:3002" };

		// Creating instances of distributed cache services
		CacheServiceInterface cache = new DistributedCacheService(server[0]);
		CacheServiceInterface cache1 = new DistributedCacheService(server[1]);
		CacheServiceInterface cache2 = new DistributedCacheService(server[2]);
		serverList = new ArrayList<CacheServiceInterface>();

		// Add the service into the list
		serverList.add(cache);
		serverList.add(cache1);
		serverList.add(cache2);
	}

	// Put values to servers

	public boolean put(long key, String value) throws InterruptedException, IOException {

		// asynchronous PUT call
		final AtomicInteger writeCount = new AtomicInteger(0);
		this.countDownLatch = new CountDownLatch(serverList.size());
		final ArrayList<CacheServiceInterface> writtenServerList = new ArrayList<CacheServiceInterface>(3);

		for (final CacheServiceInterface cacheServer : serverList) {
			//System.out.println("Task Started...." + cacheServer.returnURL());
			System.out.println("Putting values to ..." + cacheServer.returnURL());

			Future<HttpResponse<JsonNode>> future = Unirest
					.put(cacheServer.returnURL() + "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("The PUT request has failed... "+ cacheServer.returnURL());
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							writeCount.incrementAndGet();
							writtenServerList.add(cacheServer);
							System.out.println("The PUT request is successful "+ cacheServer.returnURL());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out.println("The PUT request has been cancelled");
							countDownLatch.countDown();
						}

					});
		}

		this.countDownLatch.await();

		if (writeCount.intValue() > 1) {
			return true;
		}
		else {

			// deleting current written values in case of failure

			System.out.println("Deleting...");
			this.countDownLatch = new CountDownLatch(writtenServerList.size());

			for (final CacheServiceInterface cacheServer : writtenServerList) {
				System.out.println("Deleting all the keys ===");
				Future<HttpResponse<JsonNode>> future = Unirest
						.delete(cacheServer.returnURL() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							public void failed(UnirestException e) {
								System.out.println("The Delete request failed..."+ cacheServer.returnURL());
								countDownLatch.countDown();

							}

							public void completed(
									HttpResponse<JsonNode> response) {
								System.out.println("The Delete request is successful "+ cacheServer.returnURL());
								countDownLatch.countDown();

							}

							public void cancelled() {
								System.out.println("The Delete request has been cancelled");
								countDownLatch.countDown();

							}
						});
			}
			this.countDownLatch.await(3, TimeUnit.SECONDS);

			Unirest.shutdown();
			return false;
		}
	}

	//Get values from servers

	public String get(long key) throws InterruptedException, UnirestException, IOException {

		// asynchronously GET latest values
		this.countDownLatch = new CountDownLatch(serverList.size());
		final Map<CacheServiceInterface, String> resultMap = new HashMap<CacheServiceInterface, String>();
		for (final CacheServiceInterface distributedServer : serverList) {
			Future<HttpResponse<JsonNode>> future = Unirest
					.get(distributedServer.returnURL() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("The GET request has failed");
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							resultMap.put(distributedServer, response.getBody().getObject().getString("value"));
							System.out.println("The GET request is successful "+ distributedServer.returnURL());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out.println("The GET request has been cancelled");
							countDownLatch.countDown();
						}
					});
		}

		this.countDownLatch.await(3, TimeUnit.SECONDS);
		// retrieve value with max count
		final Map<String, Integer> countMap = new HashMap<String, Integer>();
		int maxCount = 0;
		for (String value : resultMap.values()) {
			int count = 1;
			if (countMap.containsKey(value)) {
				count = countMap.get(value);
				count++;
			}
			if (maxCount < count)
				maxCount = count;
			countMap.put(value, count);
		}
		String value = this.getKeyByValue(countMap, maxCount);
		// Perform read repair
		if (maxCount != this.serverList.size()) {
			// Check from list of servers whose response is collected
			for (Entry<CacheServiceInterface, String> cacheServerData : resultMap.entrySet()) {
				if (!value.equals(cacheServerData.getValue())) {
					System.out.println("Repairing  " + cacheServerData.getKey());
					HttpResponse<JsonNode> response = Unirest
							.put(cacheServerData.getKey()
									+ "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value).asJson();
				}
			}


			// Check from list servers whose response is not collected and repair them

			for (CacheServiceInterface distributedServer : this.serverList) {
				if (resultMap.containsKey(distributedServer))
					continue;
				System.out.println("Read Repairing .." + distributedServer.returnURL());
				HttpResponse<JsonNode> response = Unirest
						.put(distributedServer.returnURL() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value).asJson();
			}
		} else {
			System.out.println("Read Repair not required");
		}
		Unirest.shutdown();
		return value;
	}

	public String getKeyByValue(Map<String, Integer> map, int value) {
		for (Entry<String, Integer> entry : map.entrySet()) {
			if (value == entry.getValue())
				return entry.getKey();
		}
		return null;
	}

}