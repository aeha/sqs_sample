package sqs_sample;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Reciever extends Thread {

	private final AmazonSQS sqs;
	private final String myQueueUrl;
	private final int ID;

	public Reciever(AmazonSQS sqs, String url, int i) {
		this.sqs = sqs;
		this.myQueueUrl = url;
		this.ID = i;
	}

	/**
	 * AWSメッセージを受信する
	 */
	@Override
	public void run() {
		for (int i = 0; i < 10; i++) {
			deleteMsg(recieveMsg());
		}
	}

	private List<Message> recieveMsg() {
		// Receive messages
		// System.out.println("Receiving messages from MyQueue.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		// 1回のリクエストで受信するメッセージを1件にする
		receiveMessageRequest.setMaxNumberOfMessages(1);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		for (Message message : messages) {
			// System.out.println(" Message");
			// System.out.println(" MessageId: " + message.getMessageId());
			// System.out.println(" ReceiptHandle: " +
			// message.getReceiptHandle());
			// System.out.println(" MD5OfBody: " + message.getMD5OfBody());
			System.out.println("ID:" + ID + "   Body:" + message.getBody());
			// for (Entry<String, String> entry :
			// message.getAttributes().entrySet()) {
			// System.out.println(" Attribute");
			// System.out.println(" Name: " + entry.getKey());
			// System.out.println(" Value: " + entry.getValue());
			// }
		}
		// System.out.println();
		return messages;
	}

	private void deleteMsg(List<Message> messages) {
		// Delete a message
		// System.out.println("Deleting a message.\n");
		if (messages.size() > 0) {
			String messageRecieptHandle = messages.get(0).getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
		}
	}
}
