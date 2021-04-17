using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                if (args.Length < 3)
                {
                    Console.WriteLine(
                    "Informe ao menos 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem, " +
                    "já no terceito em diante as mensagens a serem " +
                    "enviadas a um Topic no Kafka...");

                    return;
                }

                string topicName = args[1];

                var config = new ProducerConfig
                {
                    BootstrapServers = args[0]
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        var message = new Message<Null, string>()
                        {
                            Value = args[i]
                        };
                        var result = await producer.ProduceAsync(topicName, message);

                        Console.WriteLine(
                            $"Mensagem: {args[i]} | " +
                            $"Status: { result.Status.ToString()}");
                    }
                }

                Console.WriteLine("Concluído o envio de mensagens.");
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
