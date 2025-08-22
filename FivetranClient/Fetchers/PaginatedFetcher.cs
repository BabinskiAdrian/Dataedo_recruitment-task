using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using FivetranClient.Models;

namespace FivetranClient.Fetchers;

public sealed class PaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
    //AB a.
    private const ushort PageSize = 100;

    public IAsyncEnumerable<T> FetchItemsAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var firstPageTask = this.FetchPageAsync<T>(endpoint, cancellationToken);
        return this.ProcessPagesRecursivelyAsync(endpoint, firstPageTask, cancellationToken);
    }

    //AB
    private async Task<PaginatedRoot<T>?> FetchPageAsync<T>(
        string endpoint,
        CancellationToken cancellationToken,
        string? cursor = null)
    {
        var response = cursor is null
            ? await base.RequestHandler.GetAsync($"{endpoint}?limit={PageSize}", cancellationToken)
            : await base.RequestHandler.GetAsync($"{endpoint}?limit={PageSize}&cursor={WebUtility.UrlEncode(cursor)}", cancellationToken);
        //b.
        if(!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Expected a 2xx response but got {response.StatusCode} for endpoint: {endpoint}");

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        //c.
        if(string.IsNullOrEmpty(content))
            throw new JsonException($"Response content is empty for endpoint: {endpoint}");

        return JsonSerializer.Deserialize<PaginatedRoot<T>>(content, SerializerOptions);
    }

    // This implementation provides items as soon as they are available but also in the meantime fetches the next page
    private async IAsyncEnumerable<T> ProcessPagesRecursivelyAsync<T>(
        string endpoint,
        Task<PaginatedRoot<T>?> currentPageTask,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        //AB
        cancellationToken.ThrowIfCancellationRequested();
        var currentPage = await currentPageTask;
        //d.
        if (currentPage is null)
        {
            yield break;
            //or
            //throw new Exception();
        }
        var nextCursor = currentPage?.Data?.NextCursor;

        IAsyncEnumerable<T>? nextResults = null;
        if (!string.IsNullOrWhiteSpace(nextCursor))
        {
            // fire and forget (await after yielding current items)
            var nextTask = this.FetchPageAsync<T>(endpoint, cancellationToken, nextCursor);
            nextResults = this.ProcessPagesRecursivelyAsync(endpoint, nextTask, cancellationToken);
        }

        foreach (var item in currentPage?.Data?.Items ?? [])
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        if (nextResults is null)
            yield break;
        await foreach (var nextItem in nextResults.WithCancellation(cancellationToken))
        {
            yield return nextItem;
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
}