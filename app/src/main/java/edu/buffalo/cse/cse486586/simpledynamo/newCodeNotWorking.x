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
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class SimpleDynamoProvider extends ContentProvider {

	/* Key = <query_key###portNumOfTarget> */
	static Map<String, Boolean> isQueryAnswered = new HashMap<>();
	static Map<String, String> resultOfMyQuery = new HashMap<>();
	static Map<String, Boolean> acksReceivedForInsert = new HashMap<>();

	/* Constants, and stuff that will be set in onCreate */
	Context context;
	static int myPortNumber = 0;
	static String TAG = null;
	final String DUMMY_FILE_NAME = "DUMMY";
	static final String URI_SCHEME = "content";
	static final String URI_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static List<Integer> PORT_ID_LIST = new ArrayList<>();
	static Map<Integer, String> HASHED_PORT_ID_LIST = new HashMap<>();

	static boolean amIRecovering;
	static int recoveryAidsReceived;
	static List<Integer> deadDevices = new ArrayList<>();

	@Override
	public boolean onCreate() {

		context = getContext();

		/* Port IDs and their hashed values */
		PORT_ID_LIST.add(5562); PORT_ID_LIST.add(5556); PORT_ID_LIST.add(5554); PORT_ID_LIST.add(5558); PORT_ID_LIST.add(5560);
		HASHED_PORT_ID_LIST.put(5562, genHash(5562)); HASHED_PORT_ID_LIST.put(5556, genHash(5556)); HASHED_PORT_ID_LIST.put(5554, genHash(5554)); HASHED_PORT_ID_LIST.put(5558, genHash(5558)); HASHED_PORT_ID_LIST.put(5560, genHash(5560));

		/* Get own port ID */
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPortNumber = Integer.parseInt(portStr);
		/* Tag - to be used for all debug/error logs */
		TAG = "ANKIT-" + myPortNumber;

		/* Create a server socket and a thread (AsyncTask) that listens on the server port */
		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//new ServerTask().executeOnExecutor(Executors.newFixedThreadPool(10), serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			Log.getStackTraceString(e);
			return false;
		}

		/* Is this the 1st time or am I recovering? */
		amIRecovering = amIRecovering();
		recoveryAidsReceived = 0;

		if (amIRecovering) {
			/* Send recovering messages to everyone */
			Log.d(TAG, "[Recovery] I am recovering. Asking everyone for their stuff.");
			for (int destinationPort : PORT_ID_LIST)
				if (myPortNumber != destinationPort)
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.REQUEST_RECOVERY_INFO), String.valueOf(destinationPort));
		} else
			Log.d(TAG, "[Not recovery] I am NOT recovering. This is my first time starting up.");

		return false;
	}

	@Override
	public int delete(Uri uri, String msgKey, String[] selectionArgs) {
	    /* Only need to use the first two parameters, uri & selection */

		if (msgKey.equals("\"*\"")) {
		    /* If “*” is given as the selection parameter to delete(),
		       then you need to delete all <key, value> pairs stored in your entire DHT. */

			for (int portNum : PORT_ID_LIST)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE_STAR_REQUEST), String.valueOf(portNum));

		} else if (msgKey.equals("\"@\"")) {
		    /* Delete all files on the local partition */

			for (String key : context.fileList())
				if (!key.equals(DUMMY_FILE_NAME))
					deleteThisFile(key);
			//keysInsertedLocally.clear();

		} else {
			/* Normal case. 'selection' is the key */
			int coordinatorPortId = whereDoesItBelong(msgKey);
			List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

			Log.d(TAG, "[Delete] " + msgKey + " ==> Sending DELETE request to => " + replicaIDs);
			for (int replicaID : replicaIDs)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE_SINGLE_KEY_REQUEST), msgKey, String.valueOf(replicaID));
		}

		return 0;
	}

	private void deleteThisFile(String key) {
		// **************** TODO: Check if the delete mechanism works
		//context.deleteFile(key);
		boolean deleted = false, exists = false;

		File file = context.getFileStreamPath(key);
		exists = file.exists();
		if (exists)
			deleted = file.delete();

		if (!deleted && exists)
			Log.e("ERROR " + TAG, "Unable to delete file: " + key);
	}

	private void waiter(String msgKey, List<Integer> listOfPorts, Map<String, Boolean> ackChecker, String mode) {
		Log.v(TAG, "[" + mode + " for " + msgKey + "] " + "Waiting for everyone to reply...");
		boolean allDone, isTimedOut;
		int ackCount;
		List<Integer> devicesThatSentAcks = new ArrayList<>();
		List<Integer> devicesThatTimedOut = new ArrayList<>();
		do {
			ackCount = 0;
			allDone = false;
			isTimedOut = false;

			for (int portNum : listOfPorts) {

				/* Did we get an ack from this device? */
				boolean gotAck = ackChecker.get(msgKey + "###" + String.valueOf(portNum));
				/* Count how many ACKs have been received */
				ackCount += gotAck ? 1 : 0;
				if (gotAck && !devicesThatSentAcks.contains(portNum)) {
					devicesThatSentAcks.add(portNum);
					Log.d(TAG, "[" + mode + " for " + msgKey + "] " + portNum + " just sent an ACK.");
				}

				/* No ack from this AVD. Is it dead? */
				if (!gotAck && deadDevices.contains(portNum)) {
					isTimedOut = true;
					if (!devicesThatTimedOut.contains(portNum)) {
						devicesThatTimedOut.add(portNum);
						Log.d(TAG, "[" + mode + " for " + msgKey + "] " + portNum + " TIMED OUT");
					}
				}
			}

			if (ackCount == listOfPorts.size())
				allDone = true;

		} while (!allDone && !(isTimedOut && ackCount == listOfPorts.size()-1));

		Log.d(TAG, "[" + mode + " for " + msgKey + "] " + "Got responses from: " + devicesThatSentAcks);
		if (!devicesThatTimedOut.isEmpty())
			Log.d(TAG, "[" + mode + " for " + msgKey + "] " + "These devices TIMED OUT: " + devicesThatTimedOut);
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String msgKey, String[] selectionArgs,
	                    String sortOrder) {

		/* Make the cursor */
		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);

		if (msgKey.equals("\"*\"")) {

				/* Ask everyone for their stuff */
				for (int portNum : PORT_ID_LIST) {
					/* Key = <message_key###portNumOfTarget> */
					isQueryAnswered.put("*" + "###" + String.valueOf(portNum), false);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_STAR_REQUEST), String.valueOf(portNum));
				}

				/* -------- Wait until the query is answered by ALL */
				/* TODO: FIX here */
				waiter("*", PORT_ID_LIST, isQueryAnswered, "Query");

				/* The "*" query has been answered by all. Store the results in the cursor and return them. */
				/* Split up the key-value pairs in "resultOfMyQuery" */
				Map<String, String> resultsMap = new HashMap<>();
				for (int portNum : PORT_ID_LIST) {
					if (resultOfMyQuery.containsKey("*" + "###" + String.valueOf(portNum))) {
						String keyValuePairsString = resultOfMyQuery.get("*" + "###" + String.valueOf(portNum));
						putAllKeyValuePairsIntoMap(keyValuePairsString, resultsMap);
					}
				}

				putMapInMatrixCursor(resultsMap, matrixCursor);

				Log.v(TAG, "Query for '*' complete. No. of rows retrieved ==> " + matrixCursor.getCount());

		} else if (msgKey.equals("\"@\"")) {
			Log.v(TAG, "Query for '@' received");

			waitUntilRecoveryIsComplete("Query", "@");

			/* Return all key-value pairs on this local partition */
			for (String key : context.fileList())
				if (!key.equals(DUMMY_FILE_NAME))
					addRowToCursor(key, matrixCursor);
			Log.v(TAG, "Query for '@' complete. No. of rows retrieved ==> " + matrixCursor.getCount());

		} else {
			/* ---------------------------- Normal query (Single key) */

			/* Find out where it belongs */
			int coordinatorPortId = whereDoesItBelong(msgKey);
			List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

			Log.d(TAG, "[Query for " + msgKey + "] " + "Sending query requests to => " + replicaIDs);
			for (int replicaID : replicaIDs) {
				isQueryAnswered.put(msgKey + "###" + String.valueOf(replicaID), false);

				if (replicaID == myPortNumber) {
					Log.d(TAG, "[Query for " + msgKey + "] One of the replicas is me. Querying on self.");

					/* Wait until recovery is complete */
					waitUntilRecoveryIsComplete("Query", msgKey);
					String msgValue = readFromInternalStorage(msgKey);
					dealWithQueryResponse("x##" + msgKey + "##" + msgValue + "##" + String.valueOf(myPortNumber), msgKey, myPortNumber);
				} else
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_FIND_REQUEST), msgKey, String.valueOf(replicaID));
			}

			/* -------- Wait for ACKs or until timeout */
			waiter(msgKey, replicaIDs, isQueryAnswered, "Query");

			/* Query has been answered. */
			List<String> resultList = new ArrayList<>();
			for (int replicaID : replicaIDs)
				if (resultOfMyQuery.containsKey(msgKey + "###" + String.valueOf(replicaID)))
					resultList.add(resultOfMyQuery.get(msgKey + "###" + String.valueOf(replicaID)));

			Log.v(TAG, "[Query for " + msgKey + "] Results received ==> " + resultList);

			/* Find the most recent result (the latest version) */
			String mostRecentResult = "";
			long latestVersion = 0;
			for (String result: resultList) {
				/* Get latest version from all the replies */
				if (result != null && !result.trim().isEmpty()) {
					long thisVersion = getVersion(result);
					if (thisVersion > latestVersion) {
						latestVersion = thisVersion;
						mostRecentResult = result;
					}
				}
			}
			/* Store the result in the matrix-cursor and return them. */
			String[] columnValues = {msgKey, getActualValue(mostRecentResult)};
			matrixCursor.addRow(columnValues);

			Log.v(TAG, "[Query for " + msgKey + "] Complete. Result ==> " + mostRecentResult);
		}

		return matrixCursor;
	}

	private void waitUntilRecoveryIsComplete(String mode, String key) {
		if (amIRecovering) {
			Log.d(TAG, "[" + mode + " for " + key + "] I'm still in recovery. Waiting until recovery is complete.");
			while(amIRecovering);
			Log.d(TAG, "[" + mode + " for " + key + "] Recovery complete. Proceeding.");
		}
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

			String msgKey = (String) values.get("key");
			String msgValue = (String) values.get("value");

			/* Find out where it belongs */
			int coordinatorPortId = whereDoesItBelong(msgKey);
			List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);
			Log.d(TAG, "[Insert for " + msgKey + "] Sending insert requests to => " + replicaIDs);

			for (int replicaID : replicaIDs) {
				/* Key = <message_key###portNumOfTarget> */
				if (replicaID == myPortNumber) {
					Log.d(TAG, "[Insert for " + msgKey + "] One of the replicas is me. Inserting on self.");
					doInsertion(msgKey, msgValue);
					acksReceivedForInsert.put(msgKey + "###" + myPortNumber, true);
				} else {
					acksReceivedForInsert.put(msgKey + "###" + String.valueOf(replicaID), false);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERT_REQUEST), msgKey, msgValue, String.valueOf(replicaID));
				}
			}

			/* -------- Wait for ACKs or until timeout */
			waiter(msgKey, replicaIDs, acksReceivedForInsert, "Insert");

			return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}
	@Override
	public String getType(Uri uri) {
		return null;
	}


	/* ServerTask is an AsyncTask that should handle incoming messages. */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			try {
				while (true) {
					Socket clientSocket = serverSocket.accept();

					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String incomingString = bufferedReader.readLine();

					new BackgroundRunnerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, incomingString, clientSocket);
				}
			} catch (IOException e) {
				Log.e("ERROR " + TAG, Log.getStackTraceString(e));
			}

			return null;
		}
	}

	private class BackgroundRunnerTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... params) {

			String incomingString = (String) params[0];
			Socket clientSocket = (Socket) params[1];

			//Log.d(TAG, "Incoming ==> " + incomingString);
			String incoming[] = incomingString.split("##");
			Mode mode = Mode.valueOf(incoming[0]);

			try {
				switch (mode) {

					case INSERT_REQUEST: /* 3 */
					/* A request for insertion received.
					 * Message structure ===> <mode> ## <key> ## <value> ## <originator's port> */
						final String msgKey3 = incoming[1];
						final String msgValue3 = incoming[2];

					/* Do the insertion */
					/* Since it belongs here, write the content values to internal storage */
						Log.d(TAG, "[Insert for " + msgKey3 + "]" + " Insert request received.");
						String ackMessage3 = doInsertion(msgKey3, msgValue3);

						if (ackMessage3 == null)
							ackMessage3 = " ";

					/* Send ACK on the same socket*/
						Log.d(TAG, "[Insert for " + msgKey3 + "] Wrote the value (" + msgValue3 + "). Sending ACK.");
						sendOnExistingSocket(clientSocket, ackMessage3);
						break;

					case QUERY_FIND_REQUEST: /* 4 */
					/* The key is present here. Get it's value and inform the originator */
						String originatorPortId4 = incoming[1];
						String keyToFind4 = incoming[2];

					/* Wait until recovery is complete */
						waitUntilRecoveryIsComplete("Insert", keyToFind4);

						String queryResult5 = readFromInternalStorage(keyToFind4);
						Log.d(TAG, "[Query for " + keyToFind4 + "] This key was found here. Sending back value [" + queryResult5 + "] to originator => " + originatorPortId4);

					/* Send ACK on the same socket*/
						String messageToBeSent5 = Mode.QUERY_RESULT_FOUND.toString() + "##" + keyToFind4 + "##" + queryResult5 + "##" + String.valueOf(myPortNumber);
						sendOnExistingSocket(clientSocket, messageToBeSent5);
						break;

					case QUERY_STAR_REQUEST: /* 6 */
					/* Get ALL the key-value pairs stored on this device */
						String aggregatedResult6 = getAllMyStuff();
						Log.d(TAG, "Appended my stuff to the '*' query ==> " + aggregatedResult6);

					/* Send everything back to the originator */
						String messageToBeSent6 = Mode.QUERY_RESULT_FOUND.toString() + "##" + "*" + "##" + aggregatedResult6 + "##" + String.valueOf(myPortNumber);
						sendOnExistingSocket(clientSocket, messageToBeSent6);
						break;

					case DELETE_SINGLE_KEY_REQUEST: /* 7 */
							/* The key is on THIS partition. Delete it */
						String msgKey7 = incoming[1];

						deleteThisFile(msgKey7);
						break;

					case DELETE_STAR_REQUEST: /* 8 */
							/* Delete everything locally */

						for (String key : context.fileList())
							if (!key.equals(DUMMY_FILE_NAME))
								deleteThisFile(key);

						break;

					case REQUEST_RECOVERY_INFO: /* 10 */
					/* Someone just recovered. Mark them as NOT DEAD */
						int idOfRecoveringDevice = Integer.parseInt(incoming[1]);
						if (deadDevices.contains(Integer.valueOf(idOfRecoveringDevice)))
							deadDevices.remove(Integer.valueOf(idOfRecoveringDevice));

					/* Need to send all my stuff to the recovering AVD */
					/* Put only those key-value pairs that actually belong at the recovering AVD */
						String allMyStuff = getAllMyStuff(true, idOfRecoveringDevice);
						String messageToBeSent10 = Mode.RECOVERY_INFORMATION.toString() + "##" + allMyStuff;

						Log.d(TAG, "[Recovery] Sending my stuff to the recovering AVD [" + idOfRecoveringDevice + "] ==> " + allMyStuff);
						sendOnExistingSocket(clientSocket, messageToBeSent10);
						break;

					case RECOVERY_INFORMATION: /* 11 */

					/* [I'm the recovering AVD]
					 * Store all the stuff that's been sent to me here */
						String keyValuePairsString = incoming[1];
						String sendersPortNum11 = incoming[2];

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DO_RECOVERY), keyValuePairsString, sendersPortNum11);
						break;
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
			Socket sock = null;
			String msgKey, msgValue;
			int destinationPort;
			String messageToBeSent, incomingString;

			try {
				switch (mode) {

					case INSERT_REQUEST: /* 1 */
						/* Construct message as ===> <mode> ## <key> ## <value> */
						msgKey = (String) params[1];
						msgValue = (String) params[2];
						destinationPort = Integer.parseInt((String) params[3]);
						messageToBeSent = Mode.INSERT_REQUEST.toString() + "##" + msgKey + "##" + msgValue;

						Log.d(TAG, "[Insert for " + msgKey + "] Sending request to: " + destinationPort);
						sock = sendOnNewSocket(messageToBeSent, destinationPort * 2);

						/* ACK received (I'm the originator) */
						incomingString = readFromSocket(sock);
						if (incomingString != null) {
							Log.d(TAG, "[Insert for " + msgKey + "] ACK received from " + destinationPort + ": " + incomingString);
							String incoming[] = incomingString.split("##");
							String idOfAckSender = incoming[1];
							acksReceivedForInsert.put(msgKey + "###" + idOfAckSender, true);
						} else {
							/* Device is DEAD */
							Log.d(TAG, "[Insert for " + msgKey + "] " + destinationPort + " DIED during the insertion of key=" + msgKey + ", value=" + msgValue);
							deadDevices.add(destinationPort);
						}

						break;

					case QUERY_FIND_REQUEST: /* 2 */
						String keyToFind2 = (String) params[1];
						int destinationPortId2 = Integer.parseInt((String) params[2]);
						String originator2 = String.valueOf(myPortNumber);

						Log.d(TAG, "[Query for " + keyToFind2 + "] Sending request to: " + destinationPortId2);
						String messageToBeSent2 = Mode.QUERY_FIND_REQUEST.toString() + "##" + originator2 + "##" + keyToFind2;
						sock = sendOnNewSocket(messageToBeSent2, destinationPortId2 * 2);

						/* Read ACK */
						String incomingString2 = readFromSocket(sock);
						dealWithQueryResponse(incomingString2, keyToFind2, destinationPortId2);

						break;

					case QUERY_STAR_REQUEST: /* 3 */
						/* Ask the AVD on the destination port to add on ALL his key-value pairs */
						int destinationPortId3 = Integer.parseInt((String) params[1]);
						String originator3 = String.valueOf(myPortNumber);

						String messageToBeSent3 = Mode.QUERY_STAR_REQUEST.toString() + "##" + originator3;

						Log.d(TAG, "Sending '*' query to " + destinationPortId3);
						sock = sendOnNewSocket(messageToBeSent3, destinationPortId3 * 2);

						/* Read ACK */
						String incomingString3 = readFromSocket(sock);
						if (incomingString3 != null) {
							String incoming[] = incomingString3.split("##");
							String msgKey3 = incoming[1];
							String msgValue3 = incoming[2];
							String sendersId3 = incoming[3];

							resultOfMyQuery.put(msgKey3 + "###" + sendersId3, msgValue3);
							isQueryAnswered.put(msgKey3 + "###" + sendersId3, true);
							Log.d(TAG, "[Query result received for " + msgKey3 + "] "  + " [Sender: " + sendersId3 + "] Value = " + msgValue3);
						} else {
							/* Device is DEAD */
							Log.d(TAG, destinationPortId3 + " DIED during the querying of *");
							deadDevices.add(destinationPortId3);
						}
						break;

					case DELETE_SINGLE_KEY_REQUEST: /* 7 */
						/* Construct message as ===> <mode> ## <key> ## <value> */
						String msgKey7 = (String) params[1];
						String destinationPort7 = (String) params[2];
						String messageToBeSent7 = Mode.DELETE_SINGLE_KEY_REQUEST.toString() + "##" + msgKey7 + "##" + String.valueOf(myPortNumber);

						sock = sendOnNewSocket(messageToBeSent7, Integer.parseInt(destinationPort7) * 2);
						break;

					case DELETE_STAR_REQUEST: /* 8 */

						String destinationPort8 = (String) params[1];
						String messageToBeSent8 = Mode.DELETE_STAR_REQUEST.toString() + "##" + String.valueOf(myPortNumber);

						sock = sendOnNewSocket(messageToBeSent8, Integer.parseInt(destinationPort8) * 2);
						break;

					case REQUEST_RECOVERY_INFO: /* 9 */
						/* Ask everyone (except me) for their recovery information */
						int destinationPort9 = Integer.parseInt((String) params[1]);
						String messageToBeSent9 = Mode.REQUEST_RECOVERY_INFO.toString() + "##" + String.valueOf(myPortNumber);

						Log.d(TAG, "[Recovery] Asking " + destinationPort9 + " for it's information.");
						sock = sendOnNewSocket(messageToBeSent9, destinationPort9 * 2);

						/* Recovery response received */
						String incomingString9 = readFromSocket(sock);
						if (incomingString9 != null) {
							String incoming[] = incomingString9.split("##");

							if (incoming.length > 1) {
								String keyValuePairsString12 = incoming[1];
								Log.d(TAG, "[Recovery] Got recovery aid from " + destinationPort9 + " ==> " + keyValuePairsString12);
								Map<String, String> resultsMap = new HashMap<>();

								putAllKeyValuePairsIntoMap(keyValuePairsString12, resultsMap, true);

								for (String key : resultsMap.keySet())
									writeToInternalStorage(key, resultsMap.get(key));
							} else {
								Log.d(TAG, "[Recovery] Got recovery aid from " + destinationPort9 + " ==> [BLANK]");
							}
							if (++recoveryAidsReceived == 4) {
								Log.d(TAG, "[Recovery] Fully recovered.");
								amIRecovering = false;
							}
						} else {
							/* TODO: Device is DEAD */
						}
						break;
				}

				if (sock != null)
					sock.close();
			} catch (IOException e) {
				Log.e("ERROR " + TAG, Log.getStackTraceString(e));
			}

			return null;
		}

	}

	private void dealWithQueryResponse(String incomingString2, String keyToFind2, int destinationPortId2) {
		if (incomingString2 != null) {
			String incoming[] = incomingString2.split("##");
			String msgKey2 = incoming[1];
			String msgValue2 = incoming[2];
			String sendersId2 = incoming[3];

			resultOfMyQuery.put(msgKey2 + "###" + sendersId2, msgValue2);
			isQueryAnswered.put(msgKey2 + "###" + sendersId2, true);
			Log.d(TAG, "[Query for " + msgKey2 + "] Value received from " + sendersId2 + " [" + msgValue2  + "]");
		} else {
			/* Device is DEAD */
			Log.d(TAG, destinationPortId2 + " DIED during the querying of key=" + keyToFind2);
			deadDevices.add(destinationPortId2);
		}
	}

	private String doInsertion(String key, String value) {

		/* Wait until recovery is complete */
		waitUntilRecoveryIsComplete("Insert", key);

		/* Get the current version */
		long version = new Date().getTime();
		String currentData = readFromInternalStorage(key);
		if (!currentData.isEmpty()) {
			Log.d(TAG, "[Insert for " + key + "]" + " Existing version ==> " + getVersion(currentData) + ". New version ==> " + version);
		} else
			Log.d(TAG, "[Insert for " + key + "]" + " No existing version. New data ==> " + String.valueOf(version) + "@@@" + value);

		/* Write to storage */
		writeToInternalStorage(key, String.valueOf(version) + "@@@" + value);

		String ackMessage = key + "##" + String.valueOf(myPortNumber);
		return ackMessage;
	}


	/* ---------- UTIL methods and enums */

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

	private int whereDoesItBelong(String msgKey) {
		String hashedKey = genHash(msgKey);
		for (int portId : PORT_ID_LIST)
			if (doesItBelongHere(hashedKey, portId))
				return portId;
		return -1;
	}

	/* Does this key belong at the recovering AVD? */
	private boolean doesItBelongAtRecoveryNode(String key, int recoveringDeviceId) {
		int coordinatorId = whereDoesItBelong(key);
		List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorId, PORT_ID_LIST);

		return replicaIDs.contains(recoveringDeviceId);
	}

	private boolean doesItBelongHere(String msgKeyHashed, int nodeId) {
	    /*
	        Cases:
	          1. if I am my own successor and predecessor ---> it belongs HERE
	          2. if I'm the 1st in the ring
	                a. if msg-key > predecessor OR msg-key < my-key ---> it belongs HERE
	          3. else if msg-key is between my predecessor and me ---> it belongs HERE
	     */
		String myHashedId = HASHED_PORT_ID_LIST.get(nodeId);
		boolean belongsHere = false;

		/* Find predecessor & sucessor IDs */
		int nodeIndex = -1;
		for (int i = 0; i < PORT_ID_LIST.size(); i++)
			if (PORT_ID_LIST.get(i) == nodeId)
				nodeIndex = i;
		int predecessorId = PORT_ID_LIST.get((nodeIndex > 0) ? (nodeIndex - 1) : PORT_ID_LIST.size() - 1);
		int successorId = PORT_ID_LIST.get((nodeIndex + 1) % (PORT_ID_LIST.size()));
		String predecessorHashedId = HASHED_PORT_ID_LIST.get(predecessorId);
		String successorHashedId = HASHED_PORT_ID_LIST.get(successorId);

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

	/* buildUri() demonstrates how to build a URI for a ContentProvider. */
	public static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private long getVersion(String content) {
		return Long.parseLong(content.substring(0, content.indexOf("@@@")));
	}

	private String getActualValue(String content) {
		return content.substring(content.indexOf("@@@") + "@@@".length());
	}

	private static List<Integer> getThreeReplicaIDs(int coordinatorId, List<Integer> idList) {
		List<Integer> list = new ArrayList<>();
		int coordinatorIndex = -1;
		for (int i = 0; i < idList.size(); i++)
			if (idList.get(i) == coordinatorId)
				coordinatorIndex = i;
		list.add(idList.get(coordinatorIndex));
		list.add(idList.get((coordinatorIndex + 1) % (idList.size())));
		list.add(idList.get((coordinatorIndex + 2) % (idList.size())));
		return list;
	}

	private void addRowToCursor(String key, MatrixCursor matrixCursor) {
		String contentRead = readFromInternalStorage(key);
		/* Ignore the version part, and just return the actual value */
		String fileContent = getActualValue(contentRead);

		if (fileContent != null && fileContent.length() > 0) {
			String[] columnValues = new String[2];
			columnValues[0] = key;
			columnValues[1] = fileContent;
			matrixCursor.addRow(columnValues);
		}
		Log.d(TAG, "[Query for @] Adding key=" + key + ", value=" + contentRead + " to results.");
	}

	public String readFromSocket(Socket socket) throws IOException {
		String msgReceived = null;
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			msgReceived = bufferedReader.readLine();
		} catch (IOException e) {
			Log.e("ERROR " + TAG, Log.getStackTraceString(e));
		} finally {
			if (bufferedReader != null)
				bufferedReader.close();
		}
		return msgReceived;
	}

	/* Send to an existing socket */
	private Socket sendOnExistingSocket(Socket sendSocket, String messageToBeSent) throws IOException {
		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
			printWriter.println(messageToBeSent);
		} finally {
			if (printWriter != null)
				printWriter.close();
			if (sendSocket != null)
				sendSocket.close();

		}
		return sendSocket;
	}

	/* Send to new socket */
	private Socket sendOnNewSocket(String messageToBeSent, int destinationPort) throws IOException {
		Socket sendSocket = null;
		try {
			sendSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					destinationPort);

			PrintWriter printWriter = new PrintWriter(sendSocket.getOutputStream(), true);
			printWriter.println(messageToBeSent);

		} catch (IOException e) {
			Log.e("ERROR " + TAG, Log.getStackTraceString(e));
		}
		return sendSocket;
	}

	public boolean amIRecovering() {
		boolean amIRecovering = false;

		File file = context.getFileStreamPath(DUMMY_FILE_NAME);
		if (file.exists())
			/* File already exists */
			amIRecovering = true;
		else
			/* Doesn't exist. Make it */
			writeToInternalStorage(DUMMY_FILE_NAME, DUMMY_FILE_NAME);

		return amIRecovering;
	}

	/* Get ALL the key-value pairs stored on this device */
	public String getAllMyStuff(boolean recoveryMode, int recoveringDeviceId) {
		String allKeyValuePairs = "";
		for (String key : context.fileList()) {
			if (!key.equals(DUMMY_FILE_NAME)) {
				boolean includeThis = true;

				/* If the key doesn't belong at the recovering AVD, don't include it */
				if (recoveryMode && !doesItBelongAtRecoveryNode(key, recoveringDeviceId))
					includeThis = false;

				if (includeThis)
					allKeyValuePairs += key + "===" + readFromInternalStorage(key) + ",";
			}
		}

		/* Remove trailing comma */
		if (!allKeyValuePairs.isEmpty() && allKeyValuePairs.charAt(allKeyValuePairs.length() - 1) == ',')
			allKeyValuePairs = allKeyValuePairs.substring(0, allKeyValuePairs.length() - 1);

		return allKeyValuePairs;
	}

	public String getAllMyStuff() {
		return getAllMyStuff(false, -1);
	}

	public void putAllKeyValuePairsIntoMap(String aggregatedString, Map<String, String> resultsMap) {
		putAllKeyValuePairsIntoMap(aggregatedString, resultsMap, false);
	}
	public void putAllKeyValuePairsIntoMap(String aggregatedString, Map<String, String> resultsMap, boolean checkLocally) {

		if (aggregatedString.contains(",")) {
			String[] splitted = aggregatedString.trim().split(",");
			for (String kvPair : splitted) {

				/* kvPair is made of key===value */
				if (kvPair.contains("===")) {
					String[] keyValue = kvPair.split("===");
					String key = keyValue[0];
					String value = keyValue[1];
					long thisVersion = getVersion(value);

					/* If another AVD had already sent a value for this key,
					 * make sure we have the most recent version */
					/* No need to check this for recovery mode, because
					 * at a time only 1 AVD's stuff would be in this method*/
 					if (!checkLocally && resultsMap.containsKey(key)) {
					    String result = resultsMap.get(key);
					    if (result != null && !result.trim().isEmpty()) {
						    long existingVersion = getVersion(result);

							/* If this version is older than the existing one, keep the existing one */
						    if (thisVersion < existingVersion)
							    value = result;
					    }
					}

					/* Check if a more recent version exists locally */
					if (checkLocally) {
						String localValue = readFromInternalStorage(key);
						/* Does a local value exist for this key? */
						if (!localValue.isEmpty()) {
							long existingVersion = getVersion(localValue);

							/* If this version is older than the existing one, keep the existing one */
							if (thisVersion < existingVersion)
								value = localValue;
						}
					}

					resultsMap.put(key, value);
				}
			}
		}
	}

	public void putMapInMatrixCursor(Map<String, String> resultsMap, MatrixCursor matrixCursor) {
	/* Add rows to the matrix cursor with the key & value */
		for (String key: resultsMap.keySet()) {
			/* Ignore the version, and put the actual value */
			String actualValue = getActualValue(resultsMap.get(key));
			String[] columnValues = {key, actualValue};
			matrixCursor.addRow(columnValues);
		}
	}

	public enum Mode {
		JOIN_REQUEST, JOIN_RESPONSE, SEND_JOIN_RESPONSE, SEND_JOIN_REQUEST,
		INSERT_REQUEST, QUERY_FIND_REQUEST, QUERY_RESULT_FOUND, QUERY_STAR_REQUEST, DO_STAR_QUERY,
		DELETE_STAR_REQUEST, DELETE_SINGLE_KEY_REQUEST,
		ACK_FOR_INSERT, ACK_FOR_QUERY,
		REQUEST_RECOVERY_INFO, RECOVERY_INFORMATION, DO_RECOVERY
	}
}