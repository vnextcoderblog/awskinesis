using Amazon;
using Amazon.KinesisFirehose;
using Amazon.Runtime;
using Newtonsoft.Json;
using System.Text;

namespace producer
{
    internal class FirehoseDataProducer : IProducer
    {
        AmazonKinesisFirehoseClient _client;
        string _streamName;
        RegionEndpoint _regionEndpoint;
        ParallelOptions options;
        public FirehoseDataProducer(string streamName, RegionEndpoint regionEndpoint, int ParallelConnections)
        {
            _streamName = streamName;
            if (regionEndpoint != null)
                _regionEndpoint = regionEndpoint;
            else
                _regionEndpoint = RegionEndpoint.USEast1;

            options= new ParallelOptions()
            {
                MaxDegreeOfParallelism = ParallelConnections
            };
        }

        public void Connect(string AccessKey, string AccessSecret)
        {


            Console.WriteLine("Initializing FireHose Connectivity!");

            var cred = new BasicAWSCredentials(AccessKey, AccessSecret);
            _client = new AmazonKinesisFirehoseClient(cred, _regionEndpoint);
        }

        public async Task SendAsync(List<Customer> customers)
        {

            Console.WriteLine("Starting to Send Data!");

            await Parallel.ForEachAsync(customers, options, async (customer, ct) =>
            {

                var serializedCustomer = JsonConvert.SerializeObject(customer);

                var ms = new MemoryStream(UTF8Encoding.UTF8.GetBytes(serializedCustomer));


                try
                {
                    await _client.PutRecordAsync(_streamName, new Amazon.KinesisFirehose.Model.Record { Data = ms });
                    Console.WriteLine($"Wrote for {customer.Id}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }



            });
            Console.WriteLine("All Data Sent");
        }
    }
}
