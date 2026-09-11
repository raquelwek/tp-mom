## Middlewares Orientados a mensajes

Dado que se me pide implementar un Middleware con la interfaz definida teniendo en cuenta los siguientes conceptos:

### Exchange
Enrutan los mensjaes enviados por los productores, en este caso el intercambio 
seguirá la lóogica de enrutamiento de Topic o tópicos.

### Queue
Es el lugar de donde los consumidores agarran los mensajes recibidos. Podemos lograr esto usando las queues que provee RabbitMQ, misma versión que en DockerFile.

Para ello, declaramos una cola asociada al canal con, donde los campos, se asignan de la siguiente forma:
```
_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
//                                 durable  ad  excl  noWait, autoDelete, args
```
Así nos aseguramos de tener todos el control posible sobre la misma y que además se pueda compartir la cola entre dos Middlewares con coneconexionesxoines distintas.

#### Send
Para lograr enviar un mensaje a la cola asignda usamos `PublishWithContext` y usamos el exchange por defecto
para enviar al nombre de la cola asignado.

#### StartConsuming
Si el mensaje falla se devuelve a la cola pues requeue =true
´´´
 d.Nack(false, true)
 //     multiple, requeue
´´´