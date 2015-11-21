package sqs_sample;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SqsGetSample {

	/** 受信スレッドの数 */
	public static final int THREAD_CNT = 2;

	public static final String QUEUE_URL = "https://sqs.ap-northeast-1.amazonaws.com/465174234238/MyQueue";

	public static void main(String[] args) {
		AmazonSQS sqs = getQueue();

		// Send a message
		for (int i = 0; i < 10; i++) {
			// sqs.sendMessage(new SendMessageRequest(QUEUE_URL, "This is my
			// message text."));
			sqs.sendMessage(new SendMessageRequest(QUEUE_URL, "" + (i + 1)));
		}
		System.out.println("Sending a message to MyQueue.\n");

		for (int i = 0; i < THREAD_CNT; i++) {
			Reciever reciever = new Reciever(sqs, QUEUE_URL, i + 1);
			reciever.start();
		}

	}

	private static AmazonSQS getQueue() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
		return sqs;
	}

}
