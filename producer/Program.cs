using Amazon;
using Bogus;
using Microsoft.Extensions.Configuration;
using producer;
using System.Net;

Console.WriteLine("Setting Connectivity Options!");

var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
var builder = new ConfigurationBuilder()
    .AddJsonFile($"appsettings.json", true, true)
    .AddJsonFile($"appsettings.{env}.json", true, true)
    .AddUserSecrets<Program>()
    .AddEnvironmentVariables();

var config = builder.Build();

ServicePointManager.DefaultConnectionLimit = int.Parse(config["ParallelServiceConnections"]);

var maxThreads = int.Parse(config["MaxThreads"]);
var customersCount = int.Parse(config["CustomersCount"]);

int CustomerId = 4000;
var customerFaker = new Faker<Customer>()
    .CustomInstantiator(f => new Customer(CustomerId++))
    .RuleFor(x => x.FirstName, f => f.Name.FirstName())
    .RuleFor(x => x.LastName, f => f.Name.LastName());

var kinesisConfiguration = config.GetSection("Kinesis").Get<KinesisConfiguration>();
var firehoseConfiguration = config.GetSection("KinesisFirehose").Get<KinesisFirehoseConfiguration>();

IProducer producer = null;

if (config["Mode"] == "Kinesis")
{
    producer = new KinesisDataProducer(kinesisConfiguration.Stream, RegionEndpoint.USEast1, maxThreads);
    producer.Connect(kinesisConfiguration.AccessKey, kinesisConfiguration.AccessSecret);
}
else
{
    producer = new FirehoseDataProducer(firehoseConfiguration.Stream, RegionEndpoint.USEast1, maxThreads);
    producer.Connect(firehoseConfiguration.AccessKey, firehoseConfiguration.AccessSecret);
}
var more = "";
do
{
    var customers = customerFaker.Generate(customersCount);
    await producer.SendAsync(customers);
    Console.WriteLine("Want to send more ? Enter Y/N");
    more = Console.ReadLine();
} 
while (more.Equals("Y",StringComparison.InvariantCultureIgnoreCase));
