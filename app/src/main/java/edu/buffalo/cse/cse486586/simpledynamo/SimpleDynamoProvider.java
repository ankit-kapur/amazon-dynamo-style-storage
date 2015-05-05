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


public class SimpleDynamoProvider extends ContentProvider {

	final int TIMEOUT = 3000;

	/* Key = <query_key###portNumOfTarget> */
	static Map<String, Boolean> isQueryAnswered = new HashMap<>();
	static Map<String, String> resultOfMyQuery = new HashMap<>();
	static Map<String, Boolean> acksReceivedForInsert = new HashMap<>();
	/* TODO: Might need to make mutexes for the 3 maps above */

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
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.REQUEST_RECOVERY_INFO));
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
					context.deleteFile(key);
			//keysInsertedLocally.clear();

		} else {
			/* Normal case. 'selection' is the key */
			int coordinatorPortId = whereDoesItBelong(msgKey);
			List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

			Log.d(TAG, "[Insert] " + msgKey + " ==> Sending DELETE request to => " + replicaIDs);
			for (int replicaID : replicaIDs)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE_SINGLE_KEY_REQUEST), msgKey, String.valueOf(replicaID));
		}

		return 0;
	}

	private void waiter(String msgKey, Map<String, Long> timestamps, List<Integer> listOfPorts, Map<String, Boolean> ackChecker, String mode) {
		Log.v(TAG, "[" + mode + " for " + msgKey + "] " + "Waiting for everyone to reply...");
		boolean allDone, isTimedOut;
		List<Integer> devicesThatSentAcks = new ArrayList<>();
		do {
			int ackCount = 0;
			allDone = false;
			isTimedOut = false;

			for (int portNum : listOfPorts) {
				/* Count how many ACKs have been received */
				boolean gotAck = ackChecker.get(msgKey + "###" + String.valueOf(portNum));
				ackCount += gotAck ? 1 : 0;
				if (gotAck && !devicesThatSentAcks.contains(portNum))
					devicesThatSentAcks.add(portNum);

				/* Check if this AVD has timed out */
				long timeNow = new Date().getTime();
				if (timeNow - timestamps.get(msgKey + "###" + String.valueOf(portNum)) >= TIMEOUT) {
					isTimedOut = true;
					Log.d(TAG, "[" + mode + " for " + msgKey + "] " + "TIMEOUT on node: " + portNum);
				}
			}

			if (ackCount == listOfPorts.size())
				allDone = true;

			if (ackCount < listOfPorts.size()-1 && isTimedOut)
				Log.e(TAG, "[" + mode + " for " + msgKey + "] " + "WHY IS THERE A TIMEOUT HERE ackCount = " + ackCount + "/" + listOfPorts.size());

		} while (!allDone && !isTimedOut);

		Log.v(TAG, "[" + mode + " for " + msgKey + "] " + "Got responses from: " + devicesThatSentAcks);
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String msgKey, String[] selectionArgs,
	                    String sortOrder) {

		/* Make the cursor */
		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);

		if (msgKey.equals("\"*\"")) {

			//synchronized (this) {
				Map<String, Long> timestamps = new HashMap<>();

				/* Ask everyone for their stuff */
				for (int portNum : PORT_ID_LIST) {
					/* Key = <message_key###portNumOfTarget> */
					isQueryAnswered.put("*" + "###" + String.valueOf(portNum), false);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_STAR_REQUEST), String.valueOf(portNum));
					timestamps.put("*" + "###" + String.valueOf(portNum), new Date().getTime());
				}

				/* -------- Wait until the query is answered by ALL */
				waiter("*", timestamps, PORT_ID_LIST, isQueryAnswered, "Query");
				Log.v(TAG, "Got responses from everyone (or almost everyone) for the '*' query");

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
			//}

		} else if (msgKey.equals("\"@\"")) {
			Log.v(TAG, "Query for '@' received");

			if (amIRecovering) {
				Log.d(TAG, "[Query for @] I'm still in recovery. Waiting until recovery is complete.");
				while(amIRecovering);
				Log.d(TAG, "[Query for @] Recovery complete. Proceeding.");
			}

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

			/*  Keep track of time stamps */
			Map<String, Long> timestamps = new HashMap<>();

			Log.d(TAG, "[Query for " + msgKey + "] " + "Sending query requests to => " + replicaIDs);
			for (int replicaID : replicaIDs) {
				isQueryAnswered.put(msgKey + "###" + String.valueOf(replicaID), false);

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_FIND_REQUEST), msgKey, String.valueOf(replicaID));
				timestamps.put(msgKey + "###" + String.valueOf(replicaID), new Date().getTime());
			}

			/* -------- Wait for ACKs or until timeout */
			waiter(msgKey, timestamps, replicaIDs, isQueryAnswered, "Query");

			/* Query has been answered. */
			List<String> resultList = new ArrayList<>();
			for (int replicaID : replicaIDs)
				if (resultOfMyQuery.containsKey(msgKey + "###" + String.valueOf(replicaID)))
					resultList.add(resultOfMyQuery.get(msgKey + "###" + String.valueOf(replicaID)));

			Log.v(TAG, "[Query for " + msgKey + "] Results received ==> " + resultList);

			/* Find the most recent result (the latest version) */
			String mostRecentResult = "";
			int latestVersion = 0;
			for (String result: resultList) {
				/* Get latest version from all the replies */
				int thisVersion = getVersion(result);
				if (thisVersion > latestVersion) {
					latestVersion = thisVersion;
					mostRecentResult = result;
				}
			}
			Log.v(TAG, "[Query for " + msgKey + "] Most recent result ==> " + mostRecentResult);

			/* Store the result in the matrix-cursor and return them. */
			String[] columnValues = {msgKey, getActualValue(mostRecentResult)};
			matrixCursor.addRow(columnValues);

			Log.v(TAG, "[Query for " + msgKey + "] Complete. Result ==> " + mostRecentResult);
		}

		return matrixCursor;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		//synchronized (this) {
			String msgKey = (String) values.get("key");
			String msgValue = (String) values.get("value");

			/* Find out where it belongs */
			int coordinatorPortId = whereDoesItBelong(msgKey);
			List<Integer> replicaIDs = getThreeReplicaIDs(coordinatorPortId, PORT_ID_LIST);

			/* Keep track of time stamps */
			Map<String, Long> timestamps = new HashMap<>();

			Log.d(TAG, "[Insert for " + msgKey + "] Sending insert requests to => " + replicaIDs);
			for (int replicaID : replicaIDs) {
				/* Key = <message_key###portNumOfTarget> */
				acksReceivedForInsert.put(msgKey + "###" + String.valueOf(replicaID), false);

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERT_REQUEST), msgKey, msgValue, String.valueOf(replicaID));
				timestamps.put(msgKey + "###" + String.valueOf(replicaID), new Date().getTime());
			}

			/* -------- Wait for ACKs or until timeout */
			waiter(msgKey, timestamps, replicaIDs, acksReceivedForInsert, "Insert");
			Log.v(TAG, "[Insert for " + msgKey + "] " + "Got responses from everyone (or almost everyone)");

			return null;
		//}
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

            /* Server code that receives messages and passes them to onProgressUpdate(). */
			Socket clientSocket;

			try {
				while (true) {
					clientSocket = serverSocket.accept();

					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String incomingString = bufferedReader.readLine();

					//Log.d(TAG, "Incoming ==> " + incomingString);
					String incoming[] = incomingString.split("##");
					Mode mode = Mode.valueOf(incoming[0]);

					switch (mode) {

						case INSERT_REQUEST: /* 3 */
							/* A request for insertion received.
							 * Message structure ===> <mode> ## <key> ## <value> ## <originator's port> */

							String msgKey3 = incoming[1];
							String msgValue3 = incoming[2];
							String originatorsPortId3 = incoming[3];

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DO_INSERTION), msgKey3, msgValue3, originatorsPortId3);
							break;

						case QUERY_FIND_REQUEST: /* 4 */
							String originatorPortId4 = incoming[1];
							String keyToFind4 = incoming[2];

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY_RESULT_FOUND), originatorPortId4, keyToFind4);
							break;

						case QUERY_RESULT_FOUND: /* 5 */
							String msgKey5 = incoming[1];
							String msgValue5 = incoming[2];
							String sendersId5 = incoming[3];

							resultOfMyQuery.put(msgKey5 + "###" + sendersId5, msgValue5);
							isQueryAnswered.put(msgKey5 + "###" + sendersId5, true);
							Log.d(TAG, "[Query result found for " + msgKey5 + "] Value = " + msgValue5 + " [Sender: "+ sendersId5 + "]");
							break;

						case QUERY_STAR_REQUEST: /* 6 */
							String originatorPortId6 = incoming[1];

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DO_STAR_QUERY), originatorPortId6);
							break;

						case DELETE_SINGLE_KEY_REQUEST: /* 7 */
							/* The key is on THIS partition. Delete it */
							String msgKey7 = incoming[1];
							String originatorsPort7 = incoming[2];

							context.deleteFile(msgKey7);
							break;

						case DELETE_STAR_REQUEST: /* 8 */
							/* Delete everything locally */

							String originatorsPort8 = incoming[1];

							for (String key : context.fileList())
								if (!key.equals(DUMMY_FILE_NAME))
									context.deleteFile(key);

							break;

						case ACK_FOR_INSERT: /* 9 */
							/* ACK received (I'm the originator). Set flag = true */
							String msgKey9 = incoming[1];
							String idOfAckSender9 = incoming[2];

							/* Put the ACK in some static data structure */
							acksReceivedForInsert.put(msgKey9 + "###" + idOfAckSender9, true);
							break;

						case REQUEST_RECOVERY_INFO: /* 10 */
							String originatorPortId10 = incoming[1];

							/* Need to send all my stuff to the originator */
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.RECOVERY_INFORMATION), originatorPortId10);
							break;

						case RECOVERY_INFORMATION: /* 11 */

							/* [I'm the recovering AVD]
							 * Store all the stuff that's been sent to me here */
 							String keyValuePairsString = incoming[1];
							String sendersPortNum11 = incoming[2];

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DO_RECOVERY), keyValuePairsString, sendersPortNum11);
							break;
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
					String originatorPortId5 = (String) params[1];
					String keyToFind5 = (String) params[2];

					/* The key is present here. Get it's value and inform the originator */
					String queryResult5 = readFromInternalStorage(keyToFind5);
					Log.d(TAG, "[Query] " + keyToFind5 + " ==> belongs here. Sending back value [" + queryResult5 + "] to originator => " + originatorPortId5);

					String messageToBeSent5 = Mode.QUERY_RESULT_FOUND.toString() + "##" + keyToFind5 + "##" + queryResult5 + "##" + String.valueOf(myPortNumber);
					sendOnSocket(messageToBeSent5, Integer.parseInt(originatorPortId5) * 2);

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

				case REQUEST_RECOVERY_INFO: /* 9 */
					/* Ask everyone (except me) for their recovery information */
					String messageToBeSent9 = Mode.REQUEST_RECOVERY_INFO.toString() + "##" + String.valueOf(myPortNumber);

					for (int destinationPort9: PORT_ID_LIST)
						if (myPortNumber != destinationPort9)
							sendOnSocket(messageToBeSent9, destinationPort9 * 2);
					break;

				case RECOVERY_INFORMATION: /* 10 */
					/* Send all my stuff to the recovering AVD */
					int idOfRecoveringDevice = Integer.parseInt((String) params[1]);

					/* Put only those key-value pairs that actually belong at the recovering AVD */
					String allMyStuff = getAllMyStuff(true, idOfRecoveringDevice);
					String messageToBeSent10 = Mode.RECOVERY_INFORMATION.toString() + "##" + allMyStuff + "##" + String.valueOf(myPortNumber);
					Log.d(TAG, "[Recovery] Sending my stuff to the recovering AVD ==> " + allMyStuff);

					sendOnSocket(messageToBeSent10, idOfRecoveringDevice * 2);
					break;

				case DO_INSERTION: /* 11 */
					String msgKey11 = (String) params[1];
					String msgValue11 = (String) params[2];
					int originatorsPortId11 = Integer.parseInt((String) params[3]);

					/* Since it belongs here, write the content values to internal storage */
					Log.d(TAG, "[Insert for " + msgKey11 + "]" + " belongs here. WRITING value [" + msgValue11 + "]");

					/* Get the current version */
					int version = 1;
					String currentData = readFromInternalStorage(msgKey11);
					if (!currentData.isEmpty()) {
						int existingVersion = getVersion(currentData);
						Log.d(TAG, "[Insert for " + msgKey11 + "]" + " Existing version ==> " + existingVersion);
						version = 1 + getVersion(currentData);
					} else
						Log.d(TAG, "[Insert for " + msgKey11 + "]" + " No existing version. New data ==> " + String.valueOf(version) + "@@@" + msgValue11);

					/* Write to storage */
					writeToInternalStorage(msgKey11, String.valueOf(version) + "@@@" + msgValue11);
					//keysInsertedLocally.add(msgKey11);
					//numOfKeysInserted++;

					/* Send ACK to originator */
					String messageToBeSent11 = Mode.ACK_FOR_INSERT.toString() + "##" + msgKey11 + "##" + String.valueOf(myPortNumber);
					sendOnSocket(messageToBeSent11, originatorsPortId11 * 2);

					break;

				case DO_RECOVERY: /* 12 */
					/* Got recovery aid from someone */

					String keyValuePairsString12 = (String) params[1];
					String sendersPortNum12 = (String) params[2];
					Log.d(TAG, "[Recovery] Got recovery aid from " + sendersPortNum12 + " ==> " + keyValuePairsString12);

					Map<String, String> resultsMap = new HashMap<>();
					putAllKeyValuePairsIntoMap(keyValuePairsString12, resultsMap, true);

					for (String key: resultsMap.keySet()) {
						/* Ignore the version, and put the actual value */
						String actualValue = resultsMap.get(key);
						writeToInternalStorage(key, actualValue);
					}

					if (++recoveryAidsReceived == 4) {
						Log.d(TAG, "[Recovery] Fully recovered.");
						amIRecovering = false;
					}

					break;

				case DO_STAR_QUERY: /* 13 */
					String originatorPortId13 = (String) params[1];
					/* Get ALL the key-value pairs stored on this device */
					String aggregatedResult13 = getAllMyStuff();
					Log.d(TAG, "Appended my stuff to the '*' query ==> " + aggregatedResult13);

					/* Send everything back to the originator */
					String messageToBeSent13 = Mode.QUERY_RESULT_FOUND.toString() + "##" + "*" + "##" + aggregatedResult13 + "##" + String.valueOf(myPortNumber);
					sendOnSocket(messageToBeSent13, Integer.parseInt(originatorPortId13) * 2);
					break;
			}

			return null;
		}

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

	private int getVersion(String content) {
		return Integer.parseInt(content.substring(0, content.indexOf("@@@")));
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
					int thisVersion = getVersion(value);

					/* If another AVD had already sent a value for this key,
					 * make sure we have the most recent version */
					/* No need to check this for recovery mode, because
					 * at a time only 1 AVD's stuff would be in this method*/
 					if (!checkLocally && resultsMap.containsKey(key)) {
						int existingVersion = getVersion(resultsMap.get(key));

						/* If this version is older than the existing one, keep the existing one */
						if (thisVersion < existingVersion)
							value = resultsMap.get(key);
					}

					/* Check if a more recent version exists locally */
					if (checkLocally) {
						String localValue = readFromInternalStorage(key);
						/* Does a local value exist for this key? */
						if (!localValue.isEmpty()) {
							int existingVersion = getVersion(localValue);

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
		JOIN_REQUEST, JOIN_RESPONSE,
		SEND_JOIN_RESPONSE, SEND_JOIN_REQUEST,
		INSERT_REQUEST, DO_INSERTION,
		QUERY_FIND_REQUEST, QUERY_RESULT_FOUND, QUERY_STAR_REQUEST, DO_STAR_QUERY,
		DELETE_STAR_REQUEST, DELETE_SINGLE_KEY_REQUEST,
		ACK_FOR_INSERT, ACK_FOR_QUERY,
		REQUEST_RECOVERY_INFO, RECOVERY_INFORMATION, DO_RECOVERY
	}
}