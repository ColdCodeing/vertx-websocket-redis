package com.mm;

import com.google.common.collect.Lists;
import com.google.gxp.com.google.common.collect.Maps;
import com.mm.util.Runner;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class WebSocketRedis extends AbstractVerticle{

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketRedis.class);

    private static int uid = 0;

    private static Map<String, MyWebSocket> socketUsers = Maps.newConcurrentHashMap();
    private static Map<Integer, List<MyWebSocket>> userSockets = Maps.newConcurrentHashMap();

    private static RedisClient redis = null;

    public static void main(String[] args) {
        Runner.runExample(WebSocketRedis.class);
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String stringIn = scanner.next();
            redis.publish("channel1", stringIn, res -> {
                if (res.succeeded()) {
                    System.out.println("publish redis message ok");
                } else {
                    System.out.println("publish redis message error " + res.cause().getMessage());
                }
            });
        }
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Router router = Router.router(vertx);

        router.route().handler(StaticHandler.create("web"));

        //redis config
        Integer redisPort = config().getInteger("redis.push.port", 6379);
        String redisHost = config().getString("redis.push.host", "127.0.0.1");
        String redisAuth = config().getString("redis.push.auth");
        RedisOptions redisOptions = new RedisOptions().setHost(redisHost).setPort(redisPort);
        if (StringUtils.isNotBlank(redisAuth)) {
            redisOptions.setAuth(redisAuth);
        }
        //open redis client
        redis = RedisClient.create(vertx, redisOptions);
        //subscribe redis service
        redis.subscribe("channel1", res -> {
            if (res.succeeded()) {
                System.out.println(String.format("subscribe push channel ok, result is %s", res.result()));
            } else {
                System.out.println("can not use");
            }
        });
        //redis message handler
        vertx.eventBus().consumer("io.vertx.redis.channel1", this::receivedHandler);

        HttpServer httpServer = vertx.createHttpServer();
        HttpServer socketHttpServer = vertx.createHttpServer();

        httpServer.requestHandler(router::accept).listen(8080, httpServerAsyncResult -> {
            socketHttpServer.websocketHandler(this::serverWebSocketHandler).listen(8089, httpServerAsyncResult1 -> {
                if (httpServerAsyncResult1.succeeded()) {
                    startFuture.complete();
            } else {
                    startFuture.fail(httpServerAsyncResult1.cause());
                }
            });
        });

        vertx.setPeriodic(10000, task -> {
            System.out.println("*************************************************************");
            System.out.println("Start clear disconnected client");
            socketUsers.forEach((socketId, myWebSocket) -> {
                if (System.currentTimeMillis() / 1000 - myWebSocket.getTime() > 10) {
                    myWebSocket.getSocket().close();
                }
            });
            System.out.println(String.format("remain %s client", socketUsers.size()));
            System.out.println("*************************************************************");
        });
    }

    private void serverWebSocketHandler(ServerWebSocket serverWebSocket) {
        System.out.println("-----------------------------------------------------------------");
        System.out.println("WebSocket client connect");

        //TODO auth
        if (serverWebSocket.headers().get("Cookie") != null) {
            serverWebSocket.headers().forEach(stringStringEntry -> System.out.println(stringStringEntry.getKey() + "=" + stringStringEntry.getValue()));
            System.out.println("client id is " + uid);
            if (userSockets.containsKey(uid)) {
                userSockets.get(uid).add(new MyWebSocket(uid, serverWebSocket));
            } else {
                List<MyWebSocket> sockets =  Lists.newCopyOnWriteArrayList();
                sockets.add(new MyWebSocket(uid, serverWebSocket));
                userSockets.put(uid, sockets);
            }
            socketUsers.put(serverWebSocket.binaryHandlerID(), new MyWebSocket(uid++, serverWebSocket));
            // Set dataHandler and closeHandler for web socket As data is read, the handler will be called with the data.
            serverWebSocket.handler(buffer ->  {
                this.socketDataHandler(serverWebSocket, buffer);
            }).closeHandler(aVoid -> {
                this.socketCloseHandler(serverWebSocket);
            });
        } else {
            System.out.println("!!!!!!! reject client connect");
            serverWebSocket.reject();
        }

        System.out.println("-----------------------------------------------------------------");

    }


    private void receivedHandler(Message<JsonObject> message) {
        String value = message.body().getJsonObject("value").getString("message");
        LOGGER.info(String.format("received message %s", value));
        //TODO 1 save message
        //TODO 2 fetch user
        //TODO 3 save user-message
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        System.out.println("Start send message");
        socketUsers.forEach((socketId, myWebSocket) -> {
            try {
                myWebSocket.getSocket().writeTextMessage(String.format("message send socketId[%s] message[%s]", myWebSocket.getSocket().binaryHandlerID(), value));
            } catch (IllegalStateException e) {
                myWebSocket.getSocket().close();
            }
        });
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    }


    private void socketDataHandler(ServerWebSocket serverWebSocket ,Buffer buffer){
        socketUsers.get(serverWebSocket.binaryHandlerID()).setTime(System.currentTimeMillis() / 1000);
        System.out.println(buffer.toString());
    }

    private void socketCloseHandler(ServerWebSocket serverWebSocket) {
        System.out.println("!!!!!!!!!!!!!close handler!!!!!!!!!!!!!");
        Integer user  = socketUsers.get(serverWebSocket.binaryHandlerID()).getUid();
        if (userSockets.get(user) != null && userSockets.get(user).size() > 0) {
            userSockets.get(user).forEach(myWebSocket ->  {
                if (myWebSocket.getSocket().binaryHandlerID().equals(serverWebSocket.binaryHandlerID())) {
                    userSockets.get(user).remove(myWebSocket);
                }
            });
            if (!(userSockets.get(user) != null && userSockets.get(user).size() > 0)) {
                userSockets.remove(user);
            }
        }
        socketUsers.remove(serverWebSocket.binaryHandlerID());
    }
}


class MyWebSocket {
    private Integer uid;
    private ServerWebSocket socket;
    private Long time;

    public MyWebSocket(Integer uid, ServerWebSocket socket, long time) {
        this.uid = uid;
        this.socket = socket;
        this.time = time;
    }

    public MyWebSocket(Integer uid, ServerWebSocket socket) {
        this.uid = uid;
        this.socket = socket;
        this.time = System.currentTimeMillis() / 1000;
    }

    public ServerWebSocket getSocket() {
        return socket;
    }

    public void setSocket(ServerWebSocket socket) {
        this.socket = socket;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }
}


    //    @Override
//    public void start(Future<Void> startFuture) throws Exception {
//        Router router = Router.router(vertx);
//
//        //Allow evnets for the designated addresses in/out the enent bus bridge
//        BridgeOptions bridgeOptions = new BridgeOptions()
//                .addInboundPermitted(new PermittedOptions().setAddress("chat.to.server"))
//                .addOutboundPermitted(new PermittedOptions().setAddress("chat.to.client"));
//
//        //create the event bus bridge and add it to the router
//        SockJSHandler sockJSHandler = SockJSHandler.create(vertx).bridge(bridgeOptions);
//        router.route("/eventbus/*").handler(sockJSHandler);
//
//        //create a router end point for the static content
//        router.route().handler(StaticHandler.create("web"));
//
//
//        //start the web server and tell is to use the router to handle requests
//        vertx.createHttpServer().requestHandler(router::accept).listen(8080, result -> {
//            if (result.succeeded()) {
//                startFuture.complete();
//            } else {
//                startFuture.fail(result.cause());
//            }
//        });
//
//        EventBus eventBus = vertx.eventBus();
//
//        //register to listen for messages coming in to the server
//        eventBus.consumer("chat.to.server").handler(comingMessage -> {
//            //received message time
//            String timestamp = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM).format(Date.from(Instant.now()));
//            eventBus.publish("chat.to.client", timestamp + ": " + comingMessage.body());
//        });
//    }
//




































