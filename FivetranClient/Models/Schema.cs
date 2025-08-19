namespace FivetranClient.Models;

public class Schema
{
    public string NameInDestination { get; set; }
    public bool? Enabled { get; set; }
    //Should it be?
    // public bool? Enabled { get; set; } = false;
    public Dictionary<string, Table> Tables { get; set; }
}