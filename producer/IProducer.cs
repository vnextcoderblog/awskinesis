namespace producer;

public interface IProducer
{
    public void Connect();
    public void Connect(string AccessKey, string AccessSecret);
    public Task SendAsync(List<Customer> customers);

}

