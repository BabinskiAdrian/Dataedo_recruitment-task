using System.Net;
using System.Text.Json;
using FivetranClient.Models;

namespace FivetranClient.Fetchers;

public sealed class NonPaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
    public async Task<T?> FetchAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var response = await base.RequestHandler.GetAsync(endpoint, cancellationToken);
        //a.
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Expected a 2xx response but got {response.StatusCode} for endpoint: {endpoint}");

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        //b.
        if (string.IsNullOrEmpty(content))
            throw new JsonException($"Response content is empty for endpoint: {endpoint}");

        var root = JsonSerializer.Deserialize<NonPaginatedRoot<T>>(content, SerializerOptions);
        return root is null ? default(T) : root.Data;
    }
}


