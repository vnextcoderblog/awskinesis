using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Newtonsoft.Json;
using System.Text;

namespace producer;

internal class KinesisDataProducer : IProducer
{
    AmazonKinesisClient _client;
    string _streamName;
    RegionEndpoint _regionEndpoint;
    ParallelOptions options;
    public KinesisDataProducer(string streamName, RegionEndpoint regionEndpoint, int ParallelConnections)
    {
        _streamName = streamName;
        if (regionEndpoint != null)
            _regionEndpoint = regionEndpoint;
        else
            _regionEndpoint = RegionEndpoint.USEast1;


        options = new ParallelOptions()
        {
            MaxDegreeOfParallelism = ParallelConnections
        };
    }

    public void Connect(string AccessKey, string AccessSecret)
    {


        Console.WriteLine("Initializing Kinesis Connectivity!");

        var cred = new BasicAWSCredentials(AccessKey, AccessSecret);
        _client = new AmazonKinesisClient(cred, _regionEndpoint);
    }

    public async Task SendAsync(List<Customer> customers)
    {

        Console.WriteLine("Starting to Send Data!");

        await Parallel.ForEachAsync(customers, options,
            async (customer, cancellationToken) =>
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    var serializedCustomer = JsonConvert.SerializeObject(customer);
                    var ms = new MemoryStream(Encoding.UTF8.GetBytes(serializedCustomer));
                    try
                    {
                        var record = new PutRecordRequest()
                        {
                            Data = ms,
                            StreamName = _streamName,
                            PartitionKey = "customer"
                        };
                        await _client.PutRecordAsync(record);
                        Console.WriteLine($"Wrote for {customer.Id}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }
        );
        Console.WriteLine("All Data Sent");
    }
}
