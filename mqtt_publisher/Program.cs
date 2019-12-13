using System;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Publishing;
using MQTTnet.Client.Receiving;
using MQTTnet.Diagnostics;
using MQTTnet.Exceptions;
using MQTTnet.Internal;
using MQTTnet.Protocol;
using MessagePack.Formatters;
using System.IO;
using MessagePack;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace mqtt_test
{

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running test...");
            Task.Run(ClientTest.RunAsync);

            Thread.Sleep(Timeout.Infinite);
        }
    }

    public static class Identifier
    {
        public const int active = 0, alarm = 1, alarms = 2, asleep = 3, at = 4, auth = 5, 
            ble = 6, ble_connect = 7, ble_disconnect = 8, ble_msg = 9, cert = 10, cmd = 11,
            delete = 12, duration = 13, evt = 14,
            // Fill inn missing identifiers
            online = 38;
    }

    public class OnlineEvent
    {
        // { "14": 38, "4": 37364314838434, "13": 800 }
        public int evt { get; set; }
        public long at { get; set; }
        public long duration { get; set; }
    }
    //
    public class AlarmEvent
    {
        // Alarm event
        // The product publishes the alarm event when it detects an alarm or an alarm goes to inactive
        // state.

        //{
        //    evt: alarm,
        //    sensor: &lt;sensor_id&gt;, // Index in the global sensor array
        //    alarm: &lt;alarm_id&gt;, // Index in the per-sensor alarm array
        //    active: &lt;bool&gt;,
        //    duration: &lt;int&gt;, // How long the alarm stays active, sec
        //    timestamp: &lt;timestamp&gt;, // Timestamp of the message
        //    value: &lt;value&gt; // Actual value read from the sensor
        //}
        
        public int evt { get; set; }
        public int sensor { get; set; }
        public int alarm { get; set; }
        public bool active { get; set; }
        public long duration { get; set; }
        public DateTime timestamp { get; set; }
        public int value { get; set; }
    }

    public static class ClientTest
    {
        public static void ProsessEvent(MqttApplicationMessageReceivedEventArgs e)
        {
  
            dynamic json = MessagePackSerializer.Deserialize<dynamic>(e.ApplicationMessage.Payload, MessagePack.Resolvers.ContractlessStandardResolver.Instance);
            var data = (JObject)JsonConvert.DeserializeObject(json);
            int evt = data[Identifier.evt.ToString()].Value<int>();

            if (evt == Identifier.online)
            {
                OnlineEvent ev = new OnlineEvent
                {
                    evt = evt,
                    at = data[Identifier.at.ToString()].Value<long>(),
                    duration = data[Identifier.duration.ToString()].Value<long>()
                };
                Console.WriteLine($"<= topic: {e.ApplicationMessage.Topic}, payload: {JsonConvert.SerializeObject(ev)}");

            }
            else if (evt == Identifier.alarm)
            {
                // Logger til database
                // sender 
                Console.WriteLine("event: alarm");
            }

        }
        public static async Task RunAsync()
        {
            try
            {
                var factory = new MqttFactory();
                var client = factory.CreateMqttClient();
                var clientOptions = new MqttClientOptions
                {
                    ChannelOptions = new MqttClientTcpOptions
                    {
                        Server = "127.0.0.1"
                    }
                };


                client.ApplicationMessageReceivedHandler = new MqttApplicationMessageReceivedHandlerDelegate(e =>
                {
                    // This is the event handler 
                    // This code is executed when there is a notification on a topic we subsribe to
                    ProsessEvent(e);
                });

                client.ConnectedHandler = new MqttClientConnectedHandlerDelegate(async e =>
                {
                    Console.WriteLine("### CONNECTED WITH SERVER ###");
                    await client.SubscribeAsync(new TopicFilterBuilder().WithTopic("Reen/#").Build());
                    Console.WriteLine("### SUBSCRIBED ###");
                });

                client.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(async e =>
                {
                    Console.WriteLine("### DISCONNECTED FROM SERVER ###");
                    await Task.Delay(TimeSpan.FromSeconds(5));

                    try
                    {
                        await client.ConnectAsync(clientOptions);
                    }
                    catch
                    {
                        Console.WriteLine("### RECONNECTING FAILED ###");
                    }
                });

                try
                {
                    await client.ConnectAsync(clientOptions);
                }
                catch (Exception exception)
                {
                    Console.WriteLine("### CONNECTING FAILED ###" + Environment.NewLine + exception);
                }

                Console.WriteLine("### WAITING FOR APPLICATION MESSAGES ###");

                while (true)
                {
                    var now = DateTime.Now.Ticks;
                    var payload = "{14:38, 4:" + now + ", 13:999}";

                    // Call MessagePack Serialize.
                    //byte[] bytes = MessagePackSerializer.Serialize(alarm, MessagePack.Resolvers.ContractlessStandardResolver.Instance);
                    byte[] bytes = MessagePackSerializer.Serialize(payload, MessagePack.Resolvers.ContractlessStandardResolver.Instance);

                    var applicationMessage = new MqttApplicationMessageBuilder()
                        .WithTopic("Reen/123456/up")
                        .WithPayload(bytes)
                        .WithAtLeastOnceQoS()
                        .Build();

                    Console.WriteLine($"=> topic: {applicationMessage.Topic}, payload: {payload}");
                    await client.PublishAsync(applicationMessage);

                    Thread.Sleep(1000);
                    Console.Write("Press Ctrl-C to terminate, press Return to run test again... ");
                    Console.ReadLine();
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
        }
    }


}
