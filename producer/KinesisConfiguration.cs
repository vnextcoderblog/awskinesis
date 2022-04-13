namespace producer;

public class KinesisConfiguration
{
    public string Stream { get; set; }
    public string AccessKey { get; set; }
    public string AccessSecret { get; set; }
    public bool UseAWSProfile { get; set; }

}
