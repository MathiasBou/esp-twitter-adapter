package com.sas.coeci.esp;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import com.google.common.collect.Lists;
import com.sas.esp.api.dfESPException;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jClient;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;

public class Twitter4Esp {

	private static final String consumerKey = "dEBS9iN83ZbL6DMjj7IGiPKmY";
	private static final String consumerSecret = "PhWJj2C0WrGxyXJ8GJMAHm4q6ftJYxG0Bk0hq4RPSfs5ywu6DF";
	private static final String token = "285353386-kS3HnFISlnz3UjTYZIC8pg8Uahu1EtJBvqOGQbmL";
	private static final String secret = "HSVgIFpANURlXoJEYPmEwPgNKv9xojEYEOmFD6qpNz8fK";

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * Set up your blocking queues: Be sure to size these properly based on
		 * expected TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/**
		 * Declare the host you want to connect to, the endpoint, and
		 * authentication (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// StatusesSampleEndpoint hosebirdEndpoint = new
		// StatusesSampleEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("rdmesp", "esprdm", "rtdm", "sas_ci", "analytics"); 
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01")
				// optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint).processor(new StringDelimitedProcessor(msgQueue))
				.eventMessageQueue(eventQueue); // optional: use this if you
												// want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		// hosebirdClient.connect();

		StatusListener simpleOutputListener = new StatusListener() {

			@Override
			public void onException(Exception arg0) {

			}

			@Override
			public void onTrackLimitationNotice(int arg0) {

			}

			@Override
			public void onStatus(Status status) {
				try {
					String location = "";
					if (status.getUser().isGeoEnabled())
						location = "lat: " + status.getGeoLocation().getLatitude() + " long: " + status.getGeoLocation().getLongitude();
					String city = status.getUser().getLocation();

					System.out.println("\n\n" + "User: " + status.getUser().getScreenName() + (city.isEmpty() ? location : " from " + city) + " tweeted: \n"
							+ status.getText());
					System.out.println("\nMessage reached: " + status.getUser().getFollowersCount() + " followers");
					
					String xml11pattern = "[^" + "\u0001-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff" + "]+";
					String userLocationEsp = city.isEmpty() ? "unknown" : city;
					String legalStatusText = status.getText().replace("&", "_and_").replaceAll(xml11pattern, "");
					String legalLocationText = userLocationEsp.replace("&", "_and_").replaceAll(xml11pattern, "");
					
					
					
					ESPPublisher.publish("dfESP://localhost:55555/project/contQuery/twitterEvent", "i;n;" + status.getId() + ";"
							+ status.getUser().getScreenName() + ";" + legalStatusText + ";" + legalLocationText + ";" + status.getUser().getFollowersCount());
				} catch (SocketException e) {
					// e.printStackTrace();
				} catch (UnknownHostException e) {
					// e.printStackTrace();
				} catch (dfESPException e) {
					// e.printStackTrace();
				} catch (Exception e) {
					// e.printStackTrace();
				}

			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {

			}
		};

		List<StatusListener> listeners = new ArrayList<StatusListener>();
		listeners.add(simpleOutputListener);

		ExecutorService executorService = Executors.newFixedThreadPool(10);
		Twitter4jClient t4jClient = new Twitter4jStatusClient(hosebirdClient, msgQueue, listeners, executorService);
		t4jClient.connect();

		// Call this once for every thread you want to spin off for processing
		// the raw messages.
		// This should be called at least once.
		t4jClient.process(); // required to start processing the messages

	}

}
