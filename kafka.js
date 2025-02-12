import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { KafkaClient, KafkaMessage } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const client = new KafkaClient({
    client: {
      clientId: 'meu_app',
      brokers: ['localhost:9092'],
    },
    consumer: {
      groupId: 'meu_grupo',
    },
  });

  app.connectMicroservices();
  await app.startAllMicroservices();

  // Produtor
  const producer = client.get('meu_app');
  producer.send('mms.order', {
    evento: 'mms.order.created',
    id: 123,
    valor: 99.90
  });

  // Consumidor
  const consumer = client.subscribe('mms.order');
  consumer.on('message', (message: KafkaMessage) => {
    console.log(message.value);
  });
}

bootstrap();
