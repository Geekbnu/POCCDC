using Confluent.Kafka;
using Moq;
using Newtonsoft.Json;
using POC.Domain;
using POC.Interfaces;
using POC.Services;

namespace POC.Tests
{
    public class ProcessMessageTests
    {
        private readonly Mock<IDatabaseService> _databaseServiceMock;
        private readonly Mock<IForeignKeyService> _foreignKeyServiceMock;
        private readonly Mock<IKafkaProducerService> _producerServiceMock;
        private readonly Mock<IRedisCacheService> _redisCacheServiceMock;
        private readonly Mock<IPropertyService> _propertyServiceMock;
        private readonly Mock<IConsumer<Ignore, string>> _consumer;
        private readonly string insertFile;
        private readonly string deleteFile;

        public ProcessMessageTests()
        {
            _databaseServiceMock = new Mock<IDatabaseService>();
            _foreignKeyServiceMock = new Mock<IForeignKeyService>();
            _redisCacheServiceMock = new Mock<IRedisCacheService>();
            _propertyServiceMock = new Mock<IPropertyService>();
            _producerServiceMock = new Mock<IKafkaProducerService>();
            _consumer = new Mock<IConsumer<Ignore, string>>();

            _propertyServiceMock.Setup(p => p.GetProperty(Property.GROUPID)).Returns("test-group");
            _propertyServiceMock.Setup(p => p.GetProperty(Property.BOOTSTRAP)).Returns("localhost:9092");
            _propertyServiceMock.Setup(p => p.GetListProperty(Property.TOPICS)).Returns(new List<string> { "test-topic" });

            insertFile = File.ReadAllText("Files/insert.json");
            deleteFile = File.ReadAllText("Files/delete.json");

            _propertyServiceMock.Setup(x => x.GetListProperty(Property.TOPICS))
                .Returns(new List<string> { "test-topic" });
        }

        [Fact]
        public async Task RescheduleMessageWhenDependencyIsNotSatisfied()
        {
            // Criando uma relação entre FK na tabela em questão.
            var foreignKeyList = new List<ForeignKeyInfo>();
            foreignKeyList.Add(new ForeignKeyInfo
            {
                ForeignKeyName = "",
                ParentSchema = "",
                ParentTable = "customer", // Tabela que está referênciada no arquivo json * insert.json *
                ParentColumn = "IdTabelaPai",
                ReferencedSchema = "dbo",
                ReferencedTable = "TabelaPai",
                ReferencedColumn = "id"
            });

            _databaseServiceMock.Setup(x => x.GetDatabaseOperation(It.IsAny<char>()))
                .Returns(Enum.DatabaseOperation.CREATE);

            _databaseServiceMock.Setup(x => x.GetReferencesTables(It.IsAny<string>()))
                .ReturnsAsync(foreignKeyList);

            // O registro então não existe no banco de dados
            _databaseServiceMock.Setup(x => x.ExistsInDatabase(It.IsAny<List<KeyPar>>()))
                .ReturnsAsync(false);

            _consumer.SetupSequence(x => x.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<Ignore, string> { Message = new Message<Ignore, string> { Value = insertFile } })
                .Throws(new OperationCanceledException()); // Força a parada de consumo de mensagem.

            //Act
            var foreignKeyService = new ForeignKeyService(_databaseServiceMock.Object);

            var _consumeService = new KafkaConsumerService(_redisCacheServiceMock.Object, _propertyServiceMock.Object,
                _producerServiceMock.Object, foreignKeyService, _databaseServiceMock.Object, _consumer.Object);

            await _consumeService.ConsumeMessages();

            //Assert
            _producerServiceMock.Verify(x => x.ProduceRetryMessage(It.IsAny<TopicPartition>(), It.IsAny<string>()), Times.AtLeast(1));
        }

        [Fact]
        public async Task ExecuteOperationInDatabaseWhenThereAreNoDependency()
        {
            // Criando uma relação entre FK na tabela em questão.
            var foreignKeyList = new List<ForeignKeyInfo>();
            foreignKeyList.Add(new ForeignKeyInfo
            {
                ForeignKeyName = "",
                ParentSchema = "",
                ParentTable = "TabelaOutra", // Tabela diferente do que está referênciada no arquivo json * insert.json *
                ParentColumn = "IdTabelaPai",
                ReferencedSchema = "dbo",
                ReferencedTable = "TabelaPai",
                ReferencedColumn = "id"
            });

            _propertyServiceMock.Setup(x => x.GetListProperty(Property.TOPICS))
                .Returns(new List<string> { "test-topic" });

            _databaseServiceMock.Setup(x => x.GetDatabaseOperation(It.IsAny<char>()))
                .Returns(Enum.DatabaseOperation.CREATE);

            _databaseServiceMock.Setup(x => x.GetReferencesTables(It.IsAny<string>()))
                .ReturnsAsync(foreignKeyList);

            _consumer.SetupSequence(x => x.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<Ignore, string> { Message = new Message<Ignore, string> { Value = insertFile } })
                .Throws(new OperationCanceledException()); // Força a parada de consumo de mensagem.

            //Act
            var foreignKeyService = new ForeignKeyService(_databaseServiceMock.Object);

            var _consumeService = new KafkaConsumerService(_redisCacheServiceMock.Object, _propertyServiceMock.Object,
                _producerServiceMock.Object, foreignKeyService, _databaseServiceMock.Object, _consumer.Object);

            await _consumeService.ConsumeMessages();

            //Assert
            _databaseServiceMock.Verify(x => x.ExecuteOperationAsync(It.IsAny<string>(), It.IsAny<object>(), Enum.DatabaseOperation.CREATE), Times.AtLeast(1));
        }

        [Fact]
        public async Task DeleteRegisterWithoutCheckDependencies()

        {
            _databaseServiceMock.Setup(x => x.GetDatabaseOperation(It.IsAny<char>()))
                .Returns(Enum.DatabaseOperation.DELETE);

            _consumer.SetupSequence(x => x.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<Ignore, string> { Message = new Message<Ignore, string> { Value = deleteFile } })
                .Throws(new OperationCanceledException()); // Força a parada de consumo de mensagem.

            var _consumeService = new KafkaConsumerService(_redisCacheServiceMock.Object, _propertyServiceMock.Object,
                _producerServiceMock.Object, _foreignKeyServiceMock.Object, _databaseServiceMock.Object, _consumer.Object);

            await _consumeService.ConsumeMessages();

            //Assert
            _databaseServiceMock.Verify(x => x.ExecuteOperationAsync(It.IsAny<string>(), It.IsAny<object>(), Enum.DatabaseOperation.DELETE), Times.AtLeast(1));
        }

        [Fact]
        public void OperationDeleteFromMessage()
        {
            var databaseService = new DatabaseServiceSQLServerToPostgres(_redisCacheServiceMock.Object, _propertyServiceMock.Object);

            var data = JsonConvert.DeserializeObject<dynamic>(deleteFile);

            var operation = databaseService.GetDatabaseOperation((char)data["op"]);

            Assert.Equal(operation, Enum.DatabaseOperation.DELETE);

        }

        [Fact]
        public void OperationCreateEventFromMessage()
        {
            var databaseService = new DatabaseServiceSQLServerToPostgres(_redisCacheServiceMock.Object, _propertyServiceMock.Object);

            var data = JsonConvert.DeserializeObject<dynamic>(insertFile);

            var operation = databaseService.GetDatabaseOperation((char)data["op"]);

            Assert.Equal(operation, Enum.DatabaseOperation.CREATE);

        }


        [Fact]
        public void TableCreateEventFromMessage()
        {
            var data = JsonConvert.DeserializeObject<dynamic>(insertFile);
            var table = data["source"]["table"];

            Assert.Equal(table.ToString(), "customer");

        }

        [Fact]
        public void CreateDetailShouldBeNotNullFromMessage()
        {
            var data = JsonConvert.DeserializeObject<dynamic>(insertFile);
            var detail = data["after"]; 

            Assert.NotNull(detail);
        }

        [Fact]
        public void CreateDetailShouldBeNullFromMessage()
        {
            var data = JsonConvert.DeserializeObject<dynamic>(insertFile);
            var detail = data["before"];

            Assert.NotNull(detail);
        }


        [Fact]
        public void DeleteDetailShouldBeNotNullFromMessage()
        {
            var data = JsonConvert.DeserializeObject<dynamic>(deleteFile);
            var detail = data["before"];

            Assert.NotNull(detail);
        }
    }
}