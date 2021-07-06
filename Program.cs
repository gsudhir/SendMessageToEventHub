using System;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Collections;
using Newtonsoft.Json.Linq;

namespace SendMessageToEventHub
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://ene-rk-eventhub-003-d.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<enterSharedAccesskey>";
        private const string EventHubName = "sensorevents";
        private const string FileDirectoryPath = "C:\\Ecolab\\samplesByAssetID";
        private static int counter = 0;
        //
        public static void Main(string[] args)
        {
            //MainAsync(args).GetAwaiter().GetResult();
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };


            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            Console.WriteLine("Process Starting Time: " + DateTime.Now);

            ProcessFilesInDirectory(FileDirectoryPath);

            Console.WriteLine("Number of Messages processed: " + counter.ToString());

            Console.WriteLine("Process Ending Time: " + DateTime.Now);

            //await SendMessageToEventHub(5);

            eventHubClient.CloseAsync();


            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();

        }
        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but this simple scenario
            // uses the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            ProcessFilesInDirectory(FileDirectoryPath);

            //await SendMessageToEventHub(5);

            await eventHubClient.CloseAsync();
            

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();

        }
        //
        //
        // Uses the event hub client to send 100 messages to the event hub.
        private static async Task SendMessageToEventHub(String message)
        {
            
            try
                {
                   await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }
                await Task.Delay(10);

        }

        public static void ProcessFilesInDirectory(String path)
        {
    
                if (File.Exists(path))
                {
                    // This path is a file
                    ProcessFile(path);
                }
                else if (Directory.Exists(path))
                {
                    // This path is a directory
                    ProcessDirectory(path);
                }
                else
                {
                    Console.WriteLine("{0} is not a valid file or directory.", path);
                }

 
        }

        // Process all files in the directory passed in, recurse on any directories 
        // that are found, and process the files they contain.
        public static void ProcessDirectory(string targetDirectory)
        {
            // Process the list of files found in the directory.
            string[] fileEntries = Directory.GetFiles(targetDirectory);
            foreach (string fileName in fileEntries)
                ProcessFile(fileName);

            // Recurse into subdirectories of this directory.
            string[] subdirectoryEntries = Directory.GetDirectories(targetDirectory);
            foreach (string subdirectory in subdirectoryEntries)
                ProcessDirectory(subdirectory);
        }

        // Insert logic for processing found files here.
        public static async void ProcessFile(string path)
        {

            if (Path.GetFileName(path).Contains("json"))
            {
                var jsonText = File.ReadAllText(path);
                JArray objArray = Newtonsoft.Json.JsonConvert.DeserializeObject<JArray>(jsonText);
                if(objArray.Count > 0)
                { 
                    foreach (JObject jsonobject in objArray)
                    {
                        counter += 1;
                        //Console.WriteLine(jsonobject.ToString());
                        Console.WriteLine("sending message " + counter.ToString() + " to event hub");
                        await SendMessageToEventHub(jsonobject.ToString());
                    }
                    //Console.WriteLine("Processed file '{0}'.", Path.GetFileName(path));
                }
            }
        }
    }


}
