using System;
using System.Threading;
using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            
            if (args.Length != 3)
            {
                Console.WriteLine(
                    "Informe 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic a ser utilizado no consumo das mensagens, " +
                    "no terceiro o Group Id da aplicação...");
                return;
            }

            string bootstrapServers = args[0];
            string topicName = args[1];
            string groupId = args[2];

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(topicName);

                    try
                    {
                        while (true)
                        {                            
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine(cr.Message.Value);                            
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        Console.WriteLine("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " +
                                  $"Mensagem: {ex.Message}");
            }
        }
    }
}
