﻿namespace producer;
public class Customer
{
    public Customer(int id)
    {
        Id = id;
    }

    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
}
