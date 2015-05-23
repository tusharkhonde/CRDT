package edu.sjsu.cmpe.cache.client;


public class Client {

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Cache Client...");

		CRDT crdt = new CRDT();
		boolean requestStatus = crdt.put(1, "a");
		if (requestStatus) {
			// succeessful and GET
			Thread.sleep(10000);
			requestStatus = crdt.put(1, "b");
			if (requestStatus) {
				Thread.sleep(10000);
				String value = crdt.get(1);
				System.out.println("Server GET value " + value);
			} 
		}

		System.out.println("Existing Cache Client ...");
	}

}
