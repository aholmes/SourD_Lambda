using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;
using Npgsql;
using NpgsqlTypes;

[assembly: LambdaSerializer(typeof(LambdaJsonSerializer))]

namespace SourD
{
	public class Function
	{
		private readonly LambdaJsonSerializer serializer = new LambdaJsonSerializer();

		const int DEFAULT_MAX_INPUT_LENGTH = 500;
		private readonly int MaxInputLength;

		private readonly DbContext dbContext;

		public Function()
		{
			var env = Environment.GetEnvironmentVariables();
			dbContext = new DbContext
			(
				username: (string)env["RDS_USERNAME"],
				password: (string)env["RDS_PASSWORD"],
				hostname: (string)env["RDS_HOSTNAME"],
				port: (string)env["RDS_PORT"],
				dbName: (string)env["RDS_DB_NAME"]
			);

			if (!int.TryParse((string)env["MAX_INPUT_LENGTH"], out MaxInputLength))
			{
				MaxInputLength = DEFAULT_MAX_INPUT_LENGTH;
			}
		}

		public async Task FunctionHandler(object input, ILambdaContext context)
		{
			// re-serialize the JSON to check its length
			using var serializeStream = new MemoryStream();
			serializer.Serialize(input, serializeStream);

			// Assumes ASCII (1 byte per char)
			if (serializeStream.Length > MaxInputLength)
			{
				var message = $"Input of length {serializeStream.Length} exceeds max length of {MaxInputLength}.";
				context.Logger.Log(message);
				throw new Exception(message);
			}

			using var reader = new StreamReader(serializeStream);
			var inputAsString = await reader.ReadToEndAsync();

			using var connection = new NpgsqlConnection(dbContext.ConnectionString);
			var command = connection.CreateCommand();
			command.CommandText = "INSERT INTO app.events (data) VALUES (@input)";
			command.Parameters.AddWithValue("@input", NpgsqlDbType.Json, input);
			await connection.OpenAsync();
			await command.ExecuteNonQueryAsync();
		}

		private class DbContext
		{
			public readonly string Username;
			public readonly string Password;
			public readonly string Hostname;
			public readonly string Port;
			public readonly string DbName;

			public DbContext(string username, string password, string hostname, string port, string dbName)
			{
				Username = username;
				Password = password;
				Hostname = hostname;
				Port = port;
				DbName = dbName;
			}

			private string _connectionString;
			public string ConnectionString
			{
				get
				{
					if (_connectionString == null)
					{
						_connectionString = $"Host={Hostname};Port={Port};Database={DbName};User ID={Username};Password={Password}";
					}
					return _connectionString;
				}
			}
		}
	}
}
