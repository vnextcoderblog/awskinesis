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

var CustomerIdSeed = int.Parse(config["CustomersIdSeed"]);



var customerFaker = new Faker<Customer>()
    .CustomInstantiator(f => new Customer(CustomerIdSeed++))
    .RuleFor(x => x.FirstName, f => f.Name.FirstName())
    .RuleFor(x => x.LastName, f => f.Name.LastName());

var kinesisConfiguration = config.GetSection("KinesisConfiguration").Get<KinesisConfiguration>();

IProducer producer = null;

if (config["Mode"] == "Kinesis")
{
    producer = new KinesisDataProducer(kinesisConfiguration.Stream, RegionEndpoint.USEast1, maxThreads);
}
else if (config["Mode"] == "FireHose")
{
    producer = new FirehoseDataProducer(kinesisConfiguration.Stream, RegionEndpoint.USEast1, maxThreads);
}
else
{
    Console.WriteLine("Enter Mode as either Kinesis or FireHose in the appSettings");
    return;
}

if (kinesisConfiguration.UseAWSProfile)
    producer.Connect();
else if (! string.IsNullOrEmpty(kinesisConfiguration.AccessKey) && ! string.IsNullOrEmpty(kinesisConfiguration.AccessSecret))
    producer.Connect(kinesisConfiguration.AccessKey, kinesisConfiguration.AccessSecret);
else
{
    Console.WriteLine("Invalid Credentials.  Either specify AccessKey and AccessSecret   OR Specify UseAWSProfile as 'true' to use AWS Profile Credentials");
    return;
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
