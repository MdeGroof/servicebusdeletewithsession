using Azure.Messaging.ServiceBus;

namespace ServicebusDeleteWithSession;

class Program
{
    private const string ServiceBusConnectionString = "<< CONNECTION STRING >>";
    private const string TopicName = "<< TOPIC NAME >>";
    private const string SubscriptionName = "<< SUBSCRIPTION NAME >>";

    static async Task Main(string[] args)
    {
        // Create the Service Bus client
        await using var client = new ServiceBusClient(ServiceBusConnectionString);

        Console.WriteLine("Processing messages from sessions...");

        try
        {
            while (true)
            {
                // Accept the next available session
                var receiver = await client.AcceptNextSessionAsync(TopicName, SubscriptionName);
                if (receiver == null)
                {
                    Console.WriteLine("No more sessions available.");
                    break;
                }

                Console.WriteLine($"Processing session: {receiver.SessionId}");

                // Process messages in the session
                await ProcessSessionMessages(receiver);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred: {ex.Message}");
        }
    }

    private static async Task ProcessSessionMessages(ServiceBusSessionReceiver receiver)
    {
        try
        {
            while (true)
            {
                // Receive a message
                var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                if (message == null)
                {
                    Console.WriteLine($"No more messages in session {receiver.SessionId}.");
                    break;
                }

                // Process the message
                Console.WriteLine($"Message received in session {receiver.SessionId}: {message.Body}");

                // Complete the message (this deletes it)
                await receiver.CompleteMessageAsync(message);
                Console.WriteLine($"Message with ID {message.MessageId} from session {receiver.SessionId} deleted.");
            }
        }
        finally
        {
            // Close the receiver
            await receiver.CloseAsync();
            Console.WriteLine($"Session {receiver.SessionId} processing completed.");
        }
    }
}
