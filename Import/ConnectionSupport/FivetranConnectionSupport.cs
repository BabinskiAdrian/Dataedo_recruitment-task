using FivetranClient;
using Import.Helpers.Fivetran;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
    public const string ConnectorTypeCode = "FIVETRAN";
    private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

    //AB
    public object? GetConnectionDetailsForSelection()
    {
        Console.Write("Provide your Fivetran API Key: ");
        var apiKey = Console.ReadLine() ?? throw new ArgumentNullException();
        Console.Write("Provide your Fivetran API Secret: ");
        var apiSecret = Console.ReadLine() ?? throw new ArgumentNullException();

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }
    //a.
    public object? GetConnectionDetailsForSelection_AB()
    {
        Console.Write("Provide your Fivetran API Key: ");
        //a.
        var apiKey = Console.ReadLine();
        if(string.IsNullOrWhiteSpace(apiKey)) 
            throw new ArgumentNullException();

        Console.Write("Provide your Fivetran API Secret: ");
        var apiSecret = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(apiSecret))
            throw new ArgumentNullException();

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }

    //AB checked
    public object GetConnection(object? connectionDetails, string? selectedToImport)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }

        //a.
        if(selectedToImport is null)
            throw new ArgumentNullException(nameof(selectedToImport));

        return new RestApiManagerWrapper(
            new RestApiManager(
                details.ApiKey,
                details.ApiSecret,
                TimeSpan.FromSeconds(40)),
                selectedToImport);
    }

    //AB done
    public void CloseConnection(object? connection)
    {
        switch (connection)
        {
            case RestApiManager restApiManager:
                restApiManager.Dispose();
                break;
            case RestApiManagerWrapper restApiManagerWrapper:
                restApiManagerWrapper.Dispose();
                break;
            default:
                throw new ArgumentException("Invalid connection type provided.");
        }
    }
    //a.
    public void CloseConnection_AB(IDisposable connection)
    {
        if(connection is null)
        {
            throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");
        }   
        else
        {
            connection.Dispose();
        }
    }

    //AB checked
    public string SelectToImport(object? connectionDetails)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }
        using var restApiManager = new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(40));
        var groups = restApiManager
            .GetGroupsAsync(CancellationToken.None)
            .ToBlockingEnumerable();

        //AB a.
        var groupsCount = groups.Count();
        if (groupsCount>0)
        {
            throw new Exception("No groups found in Fivetran account.");
        }

        // bufforing for performance        
        //AB b.
        {
            var consoleOutputBufferSB = new System.Text.StringBuilder();
            consoleOutputBufferSB.AppendLine("Available groups in Fivetran account:");
            var elementIndex = 1;
            foreach (var group in groups)
            {
                consoleOutputBufferSB.AppendLine($"{elementIndex++}. {group.Name} (ID: {group.Id})");
            }
            consoleOutputBufferSB.Append("Please select a group to import from (by number): ");

            Console.Write(consoleOutputBufferSB.ToString());
        }
        
        //AB c.
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input)
            || !int.TryParse(input, out var selectedIndex)
            || selectedIndex < 1
            || selectedIndex > groupsCount)
        {
            throw new ArgumentException("Invalid group selection.");
        }

        var selectedGroup = groups.ElementAt(selectedIndex - 1);
        return selectedGroup.Id;
    }

    //AB checked
    public void RunImport(object? connection)
    {
        if (connection is not RestApiManagerWrapper restApiManagerWrapper)
        {
            throw new ArgumentException("Invalid connection type provided.");
        }

        var restApiManager = restApiManagerWrapper.RestApiManager;
        var groupId = restApiManagerWrapper.GroupId;

        var connectors = restApiManager
            .GetConnectorsAsync(groupId, CancellationToken.None)
            .ToBlockingEnumerable();
        if (!connectors.Any())
        {
            throw new Exception("No connectors found in the selected group.");
        }

        //AB a.
        {
            var allMappingsBufferSB = new System.Text.StringBuilder();
            allMappingsBufferSB.AppendLine("Lineage mappings:");
            Parallel.ForEach(connectors, connector =>
            {
                var connectorSchemas = restApiManager
                    .GetConnectorSchemasAsync(connector.Id, CancellationToken.None)
                    .Result;

                foreach (var schema in connectorSchemas?.Schemas ?? [])
                {
                    foreach (var table in schema.Value?.Tables ?? [])
                    {
                        allMappingsBufferSB.AppendLine($"  {connector.Id}: {schema.Key}.{table.Key} -> {schema.Value?.NameInDestination}.{table.Value.NameInDestination}");
                    }
                }
            });

            Console.Write(allMappingsBufferSB.ToString());
        }
    }
}