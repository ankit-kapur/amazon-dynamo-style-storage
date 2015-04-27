package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class SimpleDynamoProvider extends ContentProvider {
	/* My port number (this device's port number) */
	static int myPortNumber = 0;
	static String TAG = null;

	/* Keep track of who's alive */
	static Map<Integer, Boolean> whosAlive = new HashMap<>();
	static Map<String, Integer> versionTracker = new HashMap<>();

	Context context;
	static final int SERVER_PORT = 10000;
	static final int[] PORT_ID_LIST = {5562, 5556, 5554, 5558, 5560};

	static final String URI_SCHEME = "content";
	static final String URI_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";

	/* Important stuff */
	TreeMap<String, Integer> nodeInformation = new TreeMap<>();
	//static int predecessorId = 0, successorId = 0;
	static boolean is5554Alive = false;

	/* Key = <query_key###portNumOfTarget> */
	static Map<String, Boolean> isQueryAnswered = new HashMap<>();
	static Map<String, String> resultOfMyQuery = new HashMap<>();

	/* Map<KEY, List<PORT-ID> */
	static Map<String, List<String>> keysInserted = new HashMap<>();
	static List<String> keysInsertedLocally = new ArrayList<>();
	static int numOfKeysInserted = 0;

	@Override
	public boolean onCreate() {

		/* 	1. Get own port ID
			2. If I am 5554
				a. Declare self as predecessor and successor
				a. Create a parallel AsyncTask that:
					i. Listens for join requests
					ii. If a join request is received:
						1. Update the nodeInformation list
						2. Respond to EVERYONE, informing them of their predecessor and successor.
			3. If I'm NOT 5554
				a. Send a join request to 5554 (11108)
				b. Wait for a response
					c. If 5554 is alive - receive predecessor & successor (and set them)
					d. If 5554 is NOT alive (TIMEOUT) - declare self as predecessor & successor
		*/

		context = getContext();

		/* Get own port ID */
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPortNumber = Integer.parseInt(portStr);

		/* Get hashed ID */
		//myHashedId = genHash(String.valueOf(myPortNumber));

		/* Declare self as predecessor and successor */
		//predecessorId = myPortNumber;
		//successorId = myPortNumber;

		/* Tag - to be used for all debug/error logs */
		TAG = "ANKIT-" + myPortNumber;

		//if (myPortNumber == 5554) {
			/* Put self in the nodeInformation map */
			//nodeInformation.put(genHash(String.valueOf(myPortNumber)), myPortNumber);
		//} else {
			/* Send a join request to 5554 (11108) */
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_REQUEST));
		//}

		/* Create a server socket and a thread (AsyncTask) that listens on the server port */
		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			Log.getStackTraceString(e);
			return false;
		}

		return false;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    /* Only need to use the first two parameters, uri & selection */

		if (selection.equals("\"*\"")) {
		    /* If “*” is given as the selection parameter to delete(),
		       then you need to delete all <key, value> pairs stored in your entire DHT. */

			for (int portNum: PORT_ID_LIST)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE_STAR_REQUEST), String.valueOf(portNum));

			/* TODO: Need to do anything else? Need ACKs? */

		} else if (selection.equals("\"@\"")) {
		    /* Delete all files on the local partition */

			for (String key : keysInsertedLocally)
				context.deleteFile(key);
			keysInsertedLocally.clear();

		} else {
			/* Normal case. 'selection' is the key */
			String msgKey = selection;

			int coordinatorPortId = whereDoesItBelong(msgKey);
			int replicaIDs[] = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

			for (int replicaID: replicaIDs) {
				Log.d(TAG, "[Insert] " + msgKey + " ==> Sending DELETE request to => " + replicaID);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE_SINGLE_KEY_REQUEST), msgKey, replicaID);

				/* TODO: Need to do anything else? Need ACKs? */
				/* The ACK boolean variable must be specific to msg-key & AVD number
				 * and must be reset to false after this statement */

 			}
		}

		return 0;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
	                    String sortOrder) {

		/* Make the cursor */
		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		//Log.d(TAG, "[Query] " + "Query received. Selection ==> " + selection);

		if (selection.equals("\"*\"")) {

			/* Ask everyone for their stuff */
			for (int portNum: PORT_ID_LIST) {

				/* Key = <message_key###portNumOfTarget> */
				isQueryAnswered.put("*" + "###" + String.valueOf(portNum), false);

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_STAR_REQUEST), String.valueOf(portNum));
			}

			/* Wait until the query is answered by ALL */
			Log.v(TAG, "Waiting for EVERYONE to reply to the '*' query.");
			boolean allDone;
			do {
				allDone = true;
				for (int portNum: PORT_ID_LIST)
					allDone = allDone && isQueryAnswered.get("*" + "###" + String.valueOf(portNum));
				/* TODO: Failure scenario: Handle the case when one of the AVDs fails. This could go into an infinite loop */
			} while (!allDone);
			Log.v(TAG, "Got responses from EVERYONE for the '*' query");

			/* The "*" query has been answered by all. Store the results in the cursor and return them. */
			/* Split up the key-value pairs in "resultOfMyQuery" */
			for (int portNum: PORT_ID_LIST) {
				String result = resultOfMyQuery.get("*" + "###" + String.valueOf(portNum));
				if (result.contains(",")) {
					String[] starResults = result.trim().split(",");
					for (String starResult : starResults) {
						/* starResult is made of key===value */
						String[] keyValue = starResult.split("===");

						/* Add row to the cursor with the key & value */
						String[] columnValues = {keyValue[0], keyValue[1]};
						matrixCursor.addRow(columnValues);
					}
				}
			}

			Log.v(TAG, "Query for '*' complete. No. of rows retrieved ==> " + matrixCursor.getCount());

		} else if (selection.equals("\"@\"")) {
		    /* Handle case for "@" */

			/* Return all key-value pairs on this local partition */
			for (String key : keysInsertedLocally) {
				addRowToCursor(key, matrixCursor);
			}

			/* TODO: Seems like this is all that needs to be done. Check if it needs anything else. */
			Log.v(TAG, "Query for '@' complete. No. of rows retrieved ==> " + matrixCursor.getCount());
			Log.v(TAG, "No. of local insertions ==> " + numOfKeysInserted);

		} else {
			/* ---- Normal key ("selection" is the key) */
			String msgKey = selection;

			/* Find where it belongs */
			int coordinatorPortId = whereDoesItBelong(msgKey);

			isQueryAnswered.put(msgKey + "###" + coordinatorPortId, false);

			/* The value belongs somewhere else. Ask the successor if it has it */
			Log.d(TAG, "[Query for " + msgKey + "] ==> This key belongs at => " + coordinatorPortId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_FIND_REQUEST), msgKey, String.valueOf(coordinatorPortId));

			/* Wait until the query is answered */
			while (!isQueryAnswered.get(msgKey + "###" + coordinatorPortId));

			/* Query has been answered. Store the results in cursor and return them. */
			String[] columnValues = {selection, resultOfMyQuery.get(msgKey + "###" + coordinatorPortId)};
			matrixCursor.addRow(columnValues);

			/* ================================= TODO: Use this code */
			///* The key-value pair is here. Read it into the cursor */
			//Log.d(TAG, "[Query] " + selection + " ==> belongs here. Reading.");
			//addRowToCursor(selection, matrixCursor);

			Log.v(TAG, "[Query for " + msgKey + "] No. of rows retrieved ==> " + matrixCursor.getCount());
		}

		return matrixCursor;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String msgKey = (String) values.get("key");
		String msgValue = (String) values.get("value");

		/* Does it belong here? */
		//boolean belongsHere = doesItBelongHere(hashedKey, myPortNumber);
		int coordinatorPortId = whereDoesItBelong(msgKey);
		int replicaIDs[] = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

		Log.d(TAG, "[Insert] " + msgKey + " ==> Sending insert request to => " + replicaIDs[0] + ", " + replicaIDs[1] + ", " + replicaIDs[2]);
		for (int replicaID: replicaIDs) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERT_REQUEST), msgKey, msgValue, String.valueOf(replicaID));

			/* TODO: Wait for ACKs or until timeout */
			/* The ACK boolean variable must be specific to msg-key & AVD number
			 * and must be reset to false after this statement */


 		}

		//if (belongsHere) {
		//	/* Since it belongs here, write the content values to internal storage */
		//	Log.d(TAG, "[Insert] " + msgKey + " belongs here. Inserting.");
		//	writeToInternalStorage(msgKey, msgValue);
		//	keysInsertedLocally.add(msgKey);
		//} else {
		//	/* Doesn't belong here. Pass it on until it reaches the right place. */
		//	Log.d(TAG, "[Insert] " + msgKey + " ==> does not belong here. Passing to successor => " + successorId);
		//	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERT_REQUEST), msgKey, msgValue);
		//}

		return null;
	}

	private int whereDoesItBelong(String msgKey) {
		String hashedKey = genHash(msgKey);
		for (int portId: PORT_ID_LIST)
			if (doesItBelongHere(hashedKey, portId))
				return portId;
		return -1;
	}

	private boolean doesItBelongHere(String msgKeyHashed, int nodeId) {
	    /*
	        Cases:
	          1. if I am my own successor and predecessor ---> it belongs HERE
	          2. if I'm the 1st in the ring
	                a. if msg-key > predecessor OR msg-key < my-key ---> it belongs HERE
	          3. else if msg-key is between my predecessor and me ---> it belongs HERE
	     */
		String myHashedId = genHash(String.valueOf(nodeId));
		boolean belongsHere = false;

		/* Find predecessor & sucessor IDs */
		int nodeIndex = -1;
		for (int i=0; i< PORT_ID_LIST.length; i++)
			if (PORT_ID_LIST[i] == nodeId)
				nodeIndex = i;
		int predecessorId = PORT_ID_LIST[(nodeIndex > 0) ? (nodeIndex-1) : PORT_ID_LIST.length-1];
		int successorId = PORT_ID_LIST[(nodeIndex+1) % (PORT_ID_LIST.length)];
		String predecessorHashedId = genHash(String.valueOf(predecessorId));
		String successorHashedId = genHash(String.valueOf(successorId));

		if (myHashedId.equals(predecessorHashedId) && myHashedId.equals(successorHashedId))
			belongsHere = true;
		else if (myHashedId.compareTo(predecessorHashedId) < 0) {
			/* I'm the 1st in the ring */
			if (msgKeyHashed.compareTo(predecessorHashedId) > 0 || msgKeyHashed.compareTo(myHashedId) < 0)
				/* The message key is larger than the largest key, OR smaller than the smallest */
				belongsHere = true;
		} else if (msgKeyHashed.compareTo(predecessorHashedId) > 0 && msgKeyHashed.compareTo(myHashedId) <= 0)
			/* Normal case. The key is between my predecessor and me */
			belongsHere = true;

		//Log.d(TAG, "[belongsHere = " + belongsHere + "] hashedKey = " + msgKeyHashed + ", predecessor = " + predecessorId + "(" + predecessorHashedId + "), successor = " + successorId + "(" + successorHashedId + ")");
		return belongsHere;
	}

	//private boolean doesItBelongHere(String msgKeyHashed, int nodeId) {
	//	return doesItBelongHere(msgKeyHashed, String.valueOf(nodeId));
	//}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	private String genHash(int input) {
		return genHash(String.valueOf(input));
	}

	private String genHash(String input) {
		MessageDigest sha1;
		Formatter formatter = new Formatter();
		byte[] sha1Hash;

		try {
			sha1 = MessageDigest.getInstance("SHA-1");
			sha1Hash = sha1.digest(input.getBytes());
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
		} catch (Exception e) {
			Log.e("ERROR " + TAG, Log.getStackTraceString(e));
		}
		return formatter.toString();
	}

	private void writeToInternalStorage(String fileName, String contentOfFile) {
		try {
			FileOutputStream stream = context.openFileOutput(fileName, Context.MODE_WORLD_WRITEABLE);
			stream.write(contentOfFile.getBytes());
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String readFromInternalStorage(String fileName) {
		String contentOfFile = "";
		try {
			File file = context.getFileStreamPath(fileName);
			if (file.exists()) {
				FileInputStream stream = context.openFileInput(fileName);
				int byteContent;
				if (stream != null) {
					while ((byteContent = stream.read()) != -1)
						contentOfFile += (char) byteContent;
					stream.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentOfFile;
	}

	/* buildUri() demonstrates how to build a URI for a ContentProvider. */
	public static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	/* ServerTask is an AsyncTask that should handle incoming messages. */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

            /* Server code that receives messages and passes them to onProgressUpdate(). */
			Socket clientSocket;

			try {
				while (true) {
					clientSocket = serverSocket.accept();

					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String incomingString = bufferedReader.readLine();

					Log.d(TAG, "Incoming ==> " + incomingString);
					String incoming[] = incomingString.split("##");
					Mode mode = Mode.valueOf(incoming[0]);

					switch (mode) {
						//case JOIN_REQUEST: /* 1 */
						//	/* --- Got a join request */
						//
						//	/* Update the nodeInformation list */
						//	String sendersActualID1 = incoming[1];
						//	nodeInformation.put(genHash(sendersActualID1), Integer.parseInt(sendersActualID1));
						//
						//	/* Respond to EVERYONE, informing them of their predecessor and successor. */
						//	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SEND_JOIN_RESPONSE));
						//	break;
						//
						//case JOIN_RESPONSE: /* 2 */
						//	/* --- Got a response from 5554 for the join request I (OR SOMEONE ELSE) had sent */
						//	is5554Alive = true;
						//	Log.d(TAG, "Got neighbour information from 5554 ==> " + incomingString);
						//
						//	String IDs2 = incoming[2];
						//	String[] parts2 = IDs2.split("%%");
						//	predecessorId = Integer.parseInt(parts2[0]);
						//	successorId = Integer.parseInt(parts2[1]);
						//
						//	break;

						case INSERT_REQUEST: /* 3 */
							/* A request for insertion received.
							   Message structure ===> <mode> ## <key> ## <value> ## <originator's port> */

							String msgKey3 = incoming[1];
							String msgValue3 = incoming[2];
							String originatorsPort3 = incoming[3];

							/* Since it belongs here, write the content values to internal storage */
							Log.d(TAG, "[Insert] " + msgKey3 + " belongs here. WRITING.");
							writeToInternalStorage(msgKey3, msgValue3);
							keysInsertedLocally.add(msgKey3);
							numOfKeysInserted++;

							/* TODO: Send ACK to originator */

							break;

						case QUERY_FIND_REQUEST: /* 4 */
							// String messageToBeSent4 = Mode.QUERY_FIND_REQUEST.toString() + "##" + originator4 + "##" + keyToFind4;
							String originatorPortId4 = incoming[1];
							String keyToFind4 = incoming[2];

							/* The key is present here. Get it's value and inform the originator */
							String queryResult4 = readFromInternalStorage(keyToFind4);
							Log.d(TAG, "[Query] " + keyToFind4 + " ==> belongs here. Sending back value to originator => " + originatorPortId4);

							String messageToBeSent4 = Mode.QUERY_RESULT_FOUND.toString() + "##" + keyToFind4 + "##" + queryResult4 + "##" + String.valueOf(myPortNumber);
							sendOnSocket(messageToBeSent4, Integer.parseInt(originatorPortId4) * 2);

							break;

						case QUERY_RESULT_FOUND: /* 5 */
							String msgKey5 = incoming[1];
							String msgValue5 = incoming[2];
							String sendersId5 = incoming[3];

							resultOfMyQuery.put(msgKey5 + "###" + sendersId5, msgValue5);
							isQueryAnswered.put(msgKey5 + "###" + sendersId5, true);
							break;

						case QUERY_STAR_REQUEST: /* 6 */
							String originatorPortId6 = incoming[1];

							/* Append all your key-value pairs to the result */
							String aggregatedResult6 = "";
							for (String key6 : keysInsertedLocally) {
								String value6 = readFromInternalStorage(key6);
								aggregatedResult6 += key6 + "===" + value6 + ",";
							}

							/* Remove trailing comma */
							if (!aggregatedResult6.isEmpty() && aggregatedResult6.charAt(aggregatedResult6.length()-1) == ',')
								aggregatedResult6 = aggregatedResult6.substring(0, aggregatedResult6.length()-1);

							Log.d(TAG, "Appended my stuff to the '*' query ==> " + aggregatedResult6);

							/* Send everything back to the originator */
							String messageToBeSent6 = Mode.QUERY_RESULT_FOUND.toString() + "##" + "*" + "##" + aggregatedResult6 + "##" + String.valueOf(myPortNumber);
							sendOnSocket(messageToBeSent6, Integer.parseInt(originatorPortId6) * 2);

							break;

						case DELETE_SINGLE_KEY_REQUEST: /* 7 */
							/* The key is on THIS partition. Delete it */
							String msgKey9 = incoming[1];
							String originatorsPort9 = incoming[2];

							/* TODO: Check if this works */
							context.deleteFile(msgKey9);
							keysInsertedLocally.remove(msgKey9);

							/* TODO: Send an ACK? Not needed most probably */
							break;

						case DELETE_STAR_REQUEST: /* 8 */
							/* Delete everything locally */

							String originatorsPort10 = incoming[1];

							for (String key : keysInsertedLocally)
								context.deleteFile(key);
							keysInsertedLocally.clear();

							/* TODO: Send an ACK? Not needed most probably */
							break;

						case ACK_FOR_INSERT: /* 9 */
							/* TODO: ACK received (I'm the originator). Set flag = true */

						case ACK_FOR_QUERY: /* 10 */
							/* TODO: ACK received (I'm the originator). Set flag = true */
					}
				}
			} catch (IOException e) {
				Log.e("ERROR " + TAG, Log.getStackTraceString(e));
			}

			return null;
		}
	}

	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... params) {

			String modeString = (String) params[0];
			Mode mode = Mode.valueOf(modeString);

			switch (mode) {

				//case SEND_JOIN_REQUEST: /* 1 */
				//	/* Send join request to 5554 */
				//	int destinationPortId1 = 5554 * 2;
				//	String messageToBeSent1 = Mode.JOIN_REQUEST.toString() + "##" + myPortNumber + "##" + "null";
				//	Log.d(TAG, "Sending JOIN request to 5554 ==> " + messageToBeSent1);
				//
				//	sendOnSocket(messageToBeSent1, destinationPortId1);
				//	break;
				//
				//case SEND_JOIN_RESPONSE: /* 2 */
				//	/* Tell everyone about their neighbours */
				//	for (String hashedPortId : nodeInformation.keySet()) {
				//
				//		/* Which port are we sending the message on */
				//		int portId2 = nodeInformation.get(hashedPortId);
				//		int destinationPortId2 = portId2 * 2;
				//
				//		/* Make the message to be sent */
				//		String messageToBeSent2 = Mode.JOIN_RESPONSE.toString() + "##" + myPortNumber + "##" + getPredecessorAndSuccessor(hashedPortId);
				//
				//		Log.d(TAG, "Sending neighbour information to " + portId2 + " ==> " + messageToBeSent2);
				//
				//		/* Send the message */
				//		sendOnSocket(messageToBeSent2, destinationPortId2);
				//	}
				//
				//	break;

				case INSERT_REQUEST: /* 3 */
					/* Construct message as ===> <mode> ## <key> ## <value> */
					String msgKey3 = (String) params[1];
					String msgValue3 = (String) params[2];
					String destinationPort3 = (String) params[3];
					String messageToBeSent3 = Mode.INSERT_REQUEST.toString() + "##" + msgKey3 + "##" + msgValue3 + "##" + String.valueOf(myPortNumber);

					sendOnSocket(messageToBeSent3, Integer.parseInt(destinationPort3) * 2);
					break;

				case QUERY_FIND_REQUEST: /* 4 */
					String keyToFind4 = (String) params[1];
					int destinationPortId4 = Integer.parseInt((String) params[2]);
					String originator4 = String.valueOf(myPortNumber);

					String messageToBeSent4 = Mode.QUERY_FIND_REQUEST.toString() + "##" + originator4 + "##" + keyToFind4;

					sendOnSocket(messageToBeSent4, destinationPortId4 * 2);
					break;

				case QUERY_RESULT_FOUND: /* 5 */
					/* The key in the query was found here. Give it's value to the originator */
					//String originator5 = (String) params[1];
					//String queryResult5 = (String) params[2];
					//String messageToBeSent5 = Mode.QUERY_RESULT_FOUND.toString() + "##" + queryResult5;
					//
					//sendOnSocket(messageToBeSent5, Integer.parseInt(originator5) * 2);
					break;

				case QUERY_STAR_REQUEST: /* 6 */
					/* Ask the AVD on the destination port to add on ALL his key-value pairs */
					int destinationPortId6 = Integer.parseInt((String) params[1]);
					String originator6 = String.valueOf(myPortNumber);

					String messageToBeSent6 = Mode.QUERY_STAR_REQUEST.toString() + "##" + originator6;

					Log.d(TAG, "Sending '*' query to " + destinationPortId6 * 2);
					sendOnSocket(messageToBeSent6, destinationPortId6 * 2);
					break;

				case DELETE_SINGLE_KEY_REQUEST: /* 7 */
					/* Construct message as ===> <mode> ## <key> ## <value> */
					String msgKey7 = (String) params[1];
					String destinationPort7 = (String) params[2];
					String messageToBeSent7 = Mode.DELETE_SINGLE_KEY_REQUEST.toString() + "##" + msgKey7 + "##" + String.valueOf(myPortNumber);

					sendOnSocket(messageToBeSent7, Integer.parseInt(destinationPort7) * 2);
					break;

				case DELETE_STAR_REQUEST: /* 8 */

					String destinationPort8 = (String) params[1];
					String messageToBeSent8 = Mode.DELETE_STAR_REQUEST.toString() + "##" + String.valueOf(myPortNumber);

					sendOnSocket(messageToBeSent8, Integer.parseInt(destinationPort8) * 2);
					break;


			}

			return null;
		}

	}


	/* ---------- UTIL methods and enums */

	private static int[] getThreeReplicaIDs(int coordinatorId, int[] idList) {
		int[] list = new int[3];
		int coordinatorIndex = -1;
		for (int i=0; i< idList.length; i++)
			if (idList[i] == coordinatorId)
				coordinatorIndex = i;
		list[0] = idList[coordinatorIndex];
		list[1] = idList[(coordinatorIndex+1) % (idList.length)];
		list[2] = idList[(coordinatorIndex+2) % (idList.length)];
		return list;
	}

	private String getPredecessorAndSuccessor(String hashedPortId) {
		/* Return ==> predecessorID %% successorID */
		String predecessorAndSuccessorHashedIDs;
		String predecessorID = null, successorId;
		Iterator<String> iterator = nodeInformation.keySet().iterator();
		while (iterator.hasNext()) {
			String currentKey = iterator.next();
			if (currentKey.equals(hashedPortId))
				break;
			predecessorID = String.valueOf(nodeInformation.get(currentKey));
		}

		/* Get predecessor */
		if (predecessorID == null)
			predecessorID = String.valueOf(nodeInformation.get(nodeInformation.lastKey()));

		/* Get successor */
		if (iterator.hasNext())
			successorId = String.valueOf(nodeInformation.get(iterator.next()));
		else
			successorId = String.valueOf(nodeInformation.get(nodeInformation.firstKey()));

		/* Construct the return string */
		predecessorAndSuccessorHashedIDs = predecessorID + "%%" + successorId;

		return predecessorAndSuccessorHashedIDs;
	}

	private void addRowToCursor(String key, MatrixCursor matrixCursor) {
		String fileContent = readFromInternalStorage(key);
		if (fileContent != null && fileContent.length() > 0) {
			String[] columnValues = new String[2];
			columnValues[0] = key;
			columnValues[1] = fileContent;
			matrixCursor.addRow(columnValues);
		}
	}

	/* Send to successor */
	private void sendOnSocket(String messageToBeSent, int destinationPort) {
		Socket sendSocket = null;
		try {
			sendSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					destinationPort);
			PrintWriter printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
			printWriter.println(messageToBeSent);
		} catch (IOException e) {
			Log.e("ERROR " + TAG, Log.getStackTraceString(e));
		} finally {
			if (sendSocket != null) {
				try {
					sendSocket.close();
				} catch (IOException e) {
					Log.e("ERROR " + TAG, Log.getStackTraceString(e));
				}
			}
		}
	}

	public enum Mode {
		JOIN_REQUEST, JOIN_RESPONSE,
		SEND_JOIN_RESPONSE, SEND_JOIN_REQUEST,
		INSERT_REQUEST,
		QUERY_FIND_REQUEST, QUERY_RESULT_FOUND, QUERY_STAR_REQUEST,
		DELETE_STAR_REQUEST, DELETE_SINGLE_KEY_REQUEST,
		ACK_FOR_INSERT, ACK_FOR_QUERY,
	}
}
