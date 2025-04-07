using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace SemaphoreSlimLesson;

internal class Program
{
	static readonly string connectionString = "your_connection_string";
	static readonly string containerName = "your_container_name";
	static readonly int maxDegreeOfParallelism = 100;
	static readonly List<string> blobNames = [];
	static readonly CancellationTokenSource cts = new();

	static async Task Main(string[] args)
	{
		var containerClient = new BlobServiceClient(connectionString)
			.GetBlobContainerClient(containerName);
		var blobCount = 0;
		await foreach (var blobItem in containerClient.GetBlobsAsync())
		{
			blobNames.Add(blobItem.Name);
			Console.Write($"\rCounting blobs: {++blobCount}");
		}
		Console.WriteLine($"\nTotal blobs in container: {blobCount}");
		Console.WriteLine("The program will delete the blobs now. Press Q to quit or any other key to continue.");
		if (Console.ReadKey(true).Key == ConsoleKey.Q)
		{
			return;
		}
#pragma warning disable CS4014
		Task.Run(() =>
		{
			Console.WriteLine("Press Ctrl + Q to cancel...");
			while (true)
			{
				if (Console.KeyAvailable)
				{
					var input = Console.ReadKey(true);
					if (input.Key == ConsoleKey.Q && (input.Modifiers & ConsoleModifiers.Control) != 0)
					{
						cts.Cancel();
						break;
					}
				}
			}
		});
#pragma warning restore CS4014
		await DeleteBlobsAsync(containerClient, blobNames, cts.Token);
		Console.WriteLine("\nDone!");
	}

	private static async Task DeleteBlobsAsync(BlobContainerClient containerClient, List<string> blobNames, CancellationToken cancellationToken)
	{
		var blobCount = blobNames.Count;
		var deletedCount = 0;
		using var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
		var tasks = blobNames.Select(blobName => Task.Run(async () =>
		{
			await semaphore.WaitAsync(cancellationToken);
			try
			{
				await containerClient.DeleteBlobIfExistsAsync(blobName, DeleteSnapshotsOption.None, null, cancellationToken);
				Console.Write($"\rDeleted {++deletedCount} blobs of {blobCount}.");
			}
			catch (OperationCanceledException)
			{
				Console.Write($"\rOperation {++deletedCount - semaphore.CurrentCount} of {blobCount / maxDegreeOfParallelism} cancelled.");
			}
			finally
			{
				semaphore.Release();
			}
		}, cancellationToken)).ToList();
		try
		{
			await Task.WhenAll(tasks);
		}
		catch (OperationCanceledException)
		{
			Console.Write("\nOperation cancelled.");
		}
	}
}