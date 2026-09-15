## Middlewares Orientados a mensajes

Dado que se me pide implementar un Middleware con la interfaz definida teniendo en cuenta los siguientes conceptos:

### Exchange: Comportamiento esperado
Este modelo de envío de mensjaes busca aportar una mayor abstracción al productor/consumidor del uso de colas mediante un *Exchange*
intermedio que se encargue de esto último, de esta forma no es necesario mapear una entrada con una salida única.

Ejemplo: En la siguiente imagen se muestran dos productores, y tres consumidores conectados por un exchange.
<p align="center">
  <img src="img/ejemplo1.svg" alt="Ejemplo de diagrama">
</p>
El comportamiento esperado sería, que cualquier mensaje producido:
-  Que se rutee por A llegaría al consuidor uno y dos.
- Que se rutee por B llega al consumidor dos y tres.
Es decir que  exchange se encarga de enviar una copia a todas las colas que hayan hecho bind a esa routing key.


#### Inicio de constructor
A la hora de iniciar el middleware con exchange es necesario:
1. Declarar el exchange como durable, sin eliminación automática de esta forma el exchange perdura hasta que todos los consumers que bindearon terminen. Dejamos nowait en false, para declarar las colas en el oren correcto y internal=false para aceptar publicaciones.
2. Declara una colaanónima por consumidor, la cual se bindea a las routingkeys que desea recibir datos.
3. Adicionalmente, para el manejo de errores, se cre el canal recomendado por la documentación para transmitir errores:
```
closeErr := make(chan *rmq.Error, 1)
conn.NotifyClose(closeErr) // notifica cuando se cierra el socket
```
NOTA: Este por la librería y por ende cerrados correctamente por la misma.

#### Send
En este caso hacemos la publicación del mensaje en cada una de las routing keys con las que se inicializó el productor.
Es importante notar que se usa el exhcange inicializado y le clave de ruteo para saber hacia qué consummidor/es que bindearon con esa clave enviar el mensaje.

### Queue:  Comportamiento esperado
Es el lugar de donde los consumidores agarran los mensajes recibidos.

#### Inicio de constructor
Para lograr esto, declaramos una cola asociada al canal con, donde los campos, se asignan de la siguiente forma:
```
_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
//                                 durable  autodelete  exclusive  noWait, autoDelete, args
```
Así nos aseguramos de tener todos el control posible sobre la misma y que además se pueda compartir la cola entre dos Middlewares con con conexiones  distintas.
Es importante notar que la declaración de la cola difiere con la de el Exchange middleware en que exclusive =false, ya que de esta maneralos consumidores pueden conectarse a la misma cola de un productor y logarar el comportamiento esperado, similar a un buffer compartido.

#### Send
Para lograr enviar un mensaje a la cola asignda usamos `PublishWithContext` y usamos el exchange por defecto
para enviar al nombre de la cola asignado, siguiendo la documentación de la librería:
```
 it is possible to publish messages that route directly to this queue by publishing to "" with the routing key of the queue name.
```
Luego publicamos al default exchange con el nombre de la queue como routing key para lograr lo antes mencionado.


### Comportamiento compartido entre middlewares
Se englobo el comportamiento compartido en una struct baseMiddleware, para evitar el código repetido.
#### StartConsuming
Se consume del canal devuelto por `Consume` de la cola asociada al middleware y para un tag de un consumidor específico, se resalta la siguiente configuración:
- Manejo de acks manuales, para no perder ningún deliver.
- No exclusivo, para que pueda haber más de un consumidor conectado a la cola.
- noWait en false, para esperar que el servidor confirme la request para el consumo en este caso.

Para procesar los deliveries en una gorutina, lo que hacemos es iterar el channel hasta que se cierre, esperando nuevos mensajes de forma bloqueante hast aque se el consumidor haga StopConsuming.

#### StopConsuming
En primer lugar, verifica que ya se haya generado un tag para el consumidor, es decir ya se llamó a `StartConsuming`. Luego llama a `Canel(<tag_del_consumidor>,false)` para cerrrar el channel de deliveries antes mencionado, si no se pudo ejecutar devuleve el error correspondiente según sea el caso.


#### Close
Se asegura de cerrar de forma segura el socket (conexión) y el channel. Los canales de Confirm y Error no se cierran explícitamente, ya que su ciclo de vida es gestionado por la librería.
