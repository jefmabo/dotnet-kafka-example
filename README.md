## Running Producer
dotnet run --project Producer/Producer.csproj "localhost:9092" "topic-testes" "1" "dois" "III" "quarto" "5"

## Running Consumer
dotnet run --project Consumer/Consumer.csproj "localhost:9092" "topic-testes" "testes01"