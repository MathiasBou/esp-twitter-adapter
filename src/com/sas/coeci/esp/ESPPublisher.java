package com.sas.coeci.esp;
/* added by Mathias */

/* Standard Java imports */
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;
/* These import files are needed for all publishing code. */



import com.sas.esp.api.dfESPException;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
import com.sas.esp.api.server.eventblock.EventBlockType;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;

public class ESPPublisher {

	private static clientCallbacks clientCbListener = new clientCallbacks() {
		@Override
		public void dfESPsubscriberCB_func(dfESPeventblock arg0,
				dfESPschema arg1, Object arg2) {
		}

		@Override
		public void dfESPpubsubErrorCB_func(clientFailures arg0,
				clientFailureCodes arg1, Object arg2) {
			System.err.println("Pub-Sub Error: " + arg1.getCode() + " "
					+ arg0.toString());
		}

		@Override
		public void dfESPGDpublisherCB_func(clientGDStatus arg0, long arg1,
				Object arg2) {
		}
	};

	public static void publish(String engineUrl, String eventCsv) throws SocketException, UnknownHostException, dfESPException {

		if (engineUrl.isEmpty()) {
			engineUrl = "dfESP://sasbap.demo.sas.com:55555/project/contQuery/twitterEvent";
		}
		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(Level.ALL);

		dfESPclient client = handler.publisherStart(engineUrl,
				clientCbListener, 0);

		handler.connect(client);

		ArrayList<String> schemaVector = handler.queryMeta(engineUrl
				+ "?get=schema");
		// ArrayList<String> projectList = handler.queryMeta(engineUrl);

		// System.err.println(schemaVector.get(0));
		dfESPschema schema = new dfESPschema(schemaVector.get(0));
		/* vars for injecting event blocks */

		ArrayList<dfESPevent> eventList = new ArrayList<dfESPevent>();
		dfESPeventblock eventblock; // ESP event block to publish
		dfESPevent event;
		event = new dfESPevent(schema, eventCsv, ';');
		eventList.add(event);
		eventblock = new dfESPeventblock(eventList, EventBlockType.ebt_NORMAL);
		handler.publisherInject(client, eventblock);
		handler.stop(client, true);
	}

}
