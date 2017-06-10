using Orleans.Runtime;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Providers.RabbitMQ.Streams
{
    public class RabbitMQMessageQueueDataManager
    {
        private readonly Logger _logger;
        private readonly RabbitMQStreamProviderConfiguration _configuration;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly IBasicProperties _props;

        public RabbitMQMessageQueueDataManager(RabbitMQStreamProviderConfiguration configuration, string connectionName,
            Logger logger)
        {
            _configuration = configuration;
            var connectionFactory = configuration.ToConnectionFactory();
            _connection = connectionFactory.CreateConnection($"{connectionName}");
            _logger = logger;
            _channel = _connection.CreateModel();
            _props = GetBasicParameters();
        }

        private IBasicProperties GetBasicParameters()
        {
            var property = _channel.CreateBasicProperties();
            property.Persistent = true;
            return property;
        }

        /// <summary>
        /// Initializes the connection to the queue.
        /// </summary>
        public Task InitQueueAsync()
        {
            var startTime = DateTime.UtcNow;
            try
            {
                _channel.ExchangeDeclare(_configuration.Exchange, _configuration.ExchangeType, _configuration.ExchangeDurable, _configuration.AutoDelete, null);
                _channel.QueueDeclare(_configuration.Queue, _configuration.QueueDurable, false, false, null);
                _channel.QueueBind(_configuration.Queue, _configuration.Exchange, _configuration.RoutingKey, null);
            }
            catch (Exception ex)
            {
                ReportErrorAndRethrow(ex, "InitQueueAsync");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "InitQueueAsync");
            }
            return Task.CompletedTask;
        }

        public Task CloseQueueAsync()
        {
            var startTime = DateTime.UtcNow;
            try
            {

                if (_channel.IsOpen)
                {
                    _logger.Info(100000,"Closing RabbitMQ queue channel..");
                    _channel.Close(200, "Good Bye!");
                    _logger.Info(100000,"RabbitMQ queue channel has been clsoed!");
                }
                if (_connection.IsOpen)
                {
                    _logger.Info(100000,"Closing RabbitMQ queue connection");
                    _connection.Close(200, "Good Bye!",1000);
                    _logger.Info(100000,"RabbitMQ queue connection has been closed!");
                }
                    
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, "CloseQueueAsync");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "CloseQueueAsync");
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Deletes the queue.
        /// </summary>
        public Task DeleteQueue()
        {
            _channel.QueueDelete(_configuration.Queue);
            return TaskDone.Done;
        }

        /// <summary>
        /// Clears the queue.
        /// </summary>
        public Task ClearQueue()
        {
            _channel.QueuePurge(_configuration.Queue);
            return TaskDone.Done;
        }

        /// <summary>
        /// Adds a new message to the queue.
        /// </summary>
        /// <param name="message">Message to be added to the queue.</param>
        public async Task AddQueueMessage(RabbitMQMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                await Task.Run(() => _channel.BasicPublish(_configuration.Exchange, _configuration.RoutingKey, _props,
                    message.Body));
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }
        /// <summary>
        /// Gets a number of new messages from the queue.
        /// </summary>
        /// <param name="count">Number of messages to get from the queue.</param>
        public async Task<IEnumerable<RabbitMQMessage>> GetQueueMessages(int count = -1)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                return await Task.Run(() =>
                {
                    var results = new List<RabbitMQMessage>();
                    var i = count;
                    while (i > 0 || i == -1)
                    {
                        var result = _channel.BasicGet(_configuration.Queue, false);
                        if (result == null)
                            break;
                        results.Add(new RabbitMQMessage(result.Body, result.DeliveryTag, result.Redelivered,
                            result.Exchange, result.RoutingKey, result.BasicProperties));
                        if (i != -1)
                            i--;
                    }
                    return results.AsEnumerable();
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
                return null;
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }

        /// <summary>
        /// Deletes a messages from the queue.
        /// </summary>
        /// <param name="message">A message to be deleted from the queue.</param>
        public async Task DeleteQueueMessage(RabbitMQMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                await Task.Run(() =>
                {
                    _channel.BasicAck(message.DeliveryTag, false);
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "DeleteMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "DeleteQueueMessage");
            }
        }


        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > _configuration.QueueOperationTimeout)
            {
                _logger.Warn(100000, "Slow access to RabbitMQ queue {0} for {1}, which took {2}.", _configuration.Queue, operation, timeSpan);
            }
        }

        private void ReportErrorAndRethrow(Exception exc, string operation)
        {
            var errMsg = String.Format(
                "Error doing {0} for RabbitMQ storage queue {1} " + Environment.NewLine
                + "Exception = {2}", operation, _configuration.Queue, exc);
            _logger.Error(100000, errMsg, exc);
            throw new AggregateException(errMsg, exc);
        }

    }
}
