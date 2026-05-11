using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

var config = new ProducerConfig { BootstrapServers = "localhost:29092" };
var producer = new ProducerBuilder<Null, string>(config).Build();
var rng = new Random();
var topic = "ecommerce-events";

// симулируем пул пользователей
var userIds = Enumerable.Range(0, 100)
    .Select(_ => Guid.NewGuid())
    .ToList();

while (true)
{
    var @event = GenerateEvent(rng, userIds);
    var json = JsonSerializer.Serialize(@event);

    await producer.ProduceAsync(topic, new Message<Null, string> { Value = json });
    Console.WriteLine($"Sent: {@event.EventType} for user {@event.UserId}");

    await Task.Delay(rng.Next(100, 500)); // 2-10 событий в секунду
}

static EcommerceEvent GenerateEvent(Random rng, List<Guid> users)
{
    var userId = users[rng.Next(users.Count)];

    // реалистичное распределение: просмотров больше чем покупок
    var eventType = rng.Next(100) switch
    {
        < 60 => "item_viewed",
        < 85 => "order_created",
        < 95 => "order_paid",
        _    => "user_registered"
    };

    var payload = eventType switch
    {
        "item_viewed"  => new { item_id = rng.Next(1, 50), category = RandomCategory(rng) },
        "order_created" or "order_paid" => new { order_id = Guid.NewGuid(), amount = Math.Round(rng.NextDouble() * 10000 + 100, 2) },
        _ => new { source = "organic" } as object
    };

    return new EcommerceEvent(eventType, userId, payload, DateTime.UtcNow);
}

static string RandomCategory(Random rng) =>
    new[] { "electronics", "clothing", "food", "books" }[rng.Next(4)];

record EcommerceEvent(
    [property: JsonPropertyName("event_type")] string EventType,
    [property: JsonPropertyName("user_id")]    Guid   UserId,
    [property: JsonPropertyName("payload")]    object Payload,
    [property: JsonPropertyName("created_at")] DateTime CreatedAt
){}